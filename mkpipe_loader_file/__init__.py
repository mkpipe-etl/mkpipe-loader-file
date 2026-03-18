import gc
from datetime import datetime

from mkpipe.exceptions import ConfigError
from mkpipe.spark.base import BaseLoader
from mkpipe.spark.columns import add_etl_columns
from mkpipe.models import ConnectionConfig, ExtractResult, TableConfig
from mkpipe.utils import get_logger

JAR_PACKAGES = [
    'org.apache.hadoop:hadoop-aws:3.4.1',
    'software.amazon.awssdk:bundle:2.29.52',
    'com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.25',
    'org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.10.1',
    'org.apache.iceberg:iceberg-aws-bundle:1.10.1',
    'io.delta:delta-spark_2.13:4.0.1',
]

logger = get_logger(__name__)

SUPPORTED_FORMATS = ('parquet', 'csv', 'json', 'orc', 'avro', 'iceberg', 'delta')
SUPPORTED_CATALOGS = ('glue', 'nessie', 'rest', 'hadoop', 'unity', 'hms')


class FileLoader(BaseLoader, variant='file'):
    def __init__(self, connection: ConnectionConfig):
        self.connection = connection
        self.storage = connection.extra.get('storage', 'local')
        self.format = connection.extra.get('format', 'parquet')
        self.base_path = connection.extra.get('path', '')
        self.bucket_name = connection.bucket_name
        self.aws_access_key = connection.aws_access_key
        self.aws_secret_key = connection.aws_secret_key
        self.region = connection.region
        self.credentials_file = connection.credentials_file
        self.catalog = connection.extra.get('catalog', None)
        self.catalog_name = connection.extra.get('catalog_name', 'default')
        self.catalog_database = connection.extra.get('catalog_database', 'default')
        self.catalog_uri = connection.extra.get('catalog_uri', None)
        self.catalog_warehouse = connection.extra.get('catalog_warehouse', None)

        if self.format not in SUPPORTED_FORMATS:
            raise ConfigError(
                f"Unsupported format: '{self.format}'. Supported: {SUPPORTED_FORMATS}"
            )
        if self.catalog and self.catalog not in SUPPORTED_CATALOGS:
            raise ConfigError(
                f"Unsupported catalog: '{self.catalog}'. "
                f'Supported: {SUPPORTED_CATALOGS}'
            )

    def _resolve_path(self, table_name: str) -> str:
        if self.base_path:
            return f'{self.base_path.rstrip("/")}/{table_name}'
        if self.storage == 's3' and self.bucket_name:
            prefix = self.connection.s3_prefix or ''
            return f's3a://{self.bucket_name}/{prefix.strip("/")}/{table_name}'
        return table_name

    def _configure_storage(self, spark):
        hadoop = spark.sparkContext._jsc.hadoopConfiguration()
        if self.storage == 's3':
            if self.aws_access_key:
                hadoop.set('fs.s3a.access.key', self.aws_access_key)
                hadoop.set('fs.s3a.secret.key', self.aws_secret_key or '')
            if self.region:
                hadoop.set('fs.s3a.endpoint.region', self.region)

            # Set timeout values as milliseconds (not "60s" format)
            hadoop.set('fs.s3a.connection.timeout', '60000')
            hadoop.set('fs.s3a.connection.establish.timeout', '60000')
            hadoop.set('fs.s3a.attempts.maximum', '10')
            hadoop.set('fs.s3a.retry.limit', '5')
        elif self.storage == 'gcs':
            if self.credentials_file:
                hadoop.set(
                    'google.cloud.auth.service.account.json.keyfile',
                    self.credentials_file,
                )

    def _configure_delta_catalog(self, spark):
        """Register a Delta catalog (Unity Catalog or Hive Metastore) in the active Spark session."""
        sc = spark.conf
        name = self.catalog_name

        sc.set(
            'spark.sql.catalog.spark_catalog',
            'org.apache.spark.sql.delta.catalog.DeltaCatalog',
        )

        if self.catalog == 'unity':
            sc.set(
                f'spark.sql.catalog.{name}',
                'com.databricks.spark.sql.catalog.UnitySessionCatalog',
            )
            if self.catalog_uri:
                sc.set('spark.databricks.unityCatalog.enabled', 'true')
                sc.set('spark.databricks.unityCatalog.host', self.catalog_uri)
            token = self.connection.extra.get('unity_token', None)
            if token:
                sc.set('spark.databricks.unityCatalog.token', token)

        elif self.catalog == 'hms':
            sc.set(
                f'spark.sql.catalog.{name}',
                'org.apache.spark.sql.delta.catalog.DeltaCatalog',
            )
            if self.catalog_uri:
                sc.set('spark.hadoop.hive.metastore.uris', self.catalog_uri)

        # Note: spark.sql.extensions is a static config and must be set during SparkSession creation
        try:
            extensions = sc.get('spark.sql.extensions', '')
            if 'DeltaSparkSessionExtension' not in extensions:
                logger.warning(
                    {
                        'message': 'Delta extensions not found in spark.sql.extensions',
                        'hint': 'Add to SparkSession config: spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension',
                    }
                )
        except Exception:
            pass

    def _configure_catalog(self, spark):
        """Register the Iceberg catalog in the active Spark session."""
        sc = spark.conf
        name = self.catalog_name

        if self.catalog == 'glue':
            sc.set(f'spark.sql.catalog.{name}', 'org.apache.iceberg.spark.SparkCatalog')
            sc.set(
                f'spark.sql.catalog.{name}.catalog-impl',
                'org.apache.iceberg.aws.glue.GlueCatalog',
            )
            sc.set(
                f'spark.sql.catalog.{name}.io-impl',
                'org.apache.iceberg.aws.s3.S3FileIO',
            )
            if self.catalog_warehouse:
                sc.set(f'spark.sql.catalog.{name}.warehouse', self.catalog_warehouse)
            if self.region:
                sc.set(f'spark.sql.catalog.{name}.glue.region', self.region)
                sc.set(f'spark.sql.catalog.{name}.client.region', self.region)
            if self.aws_access_key:
                sc.set(
                    f'spark.sql.catalog.{name}.s3.access-key-id', self.aws_access_key
                )
                sc.set(
                    f'spark.sql.catalog.{name}.s3.secret-access-key',
                    self.aws_secret_key or '',
                )
            # AWS SDK v2 DefaultCredentialsProvider uses SystemPropertyCredentialsProvider
            # which reads from Java system properties. Set them so Glue client can authenticate.
            jvm = spark.sparkContext._jvm
            sys_props = jvm.java.lang.System
            if self.aws_access_key:
                sys_props.setProperty('aws.accessKeyId', self.aws_access_key)
                sys_props.setProperty('aws.secretAccessKey', self.aws_secret_key or '')
            if self.region:
                sys_props.setProperty('aws.region', self.region)

        elif self.catalog == 'nessie':
            sc.set(f'spark.sql.catalog.{name}', 'org.apache.iceberg.spark.SparkCatalog')
            sc.set(
                f'spark.sql.catalog.{name}.catalog-impl',
                'org.apache.iceberg.nessie.NessieCatalog',
            )
            sc.set(
                f'spark.sql.catalog.{name}.io-impl',
                'org.apache.iceberg.aws.s3.S3FileIO',
            )
            if self.catalog_uri:
                sc.set(f'spark.sql.catalog.{name}.uri', self.catalog_uri)
            if self.catalog_warehouse:
                sc.set(f'spark.sql.catalog.{name}.warehouse', self.catalog_warehouse)
            ref = self.connection.extra.get('nessie_ref', 'main')
            sc.set(f'spark.sql.catalog.{name}.ref', ref)
            auth_type = self.connection.extra.get('nessie_auth_type', 'NONE')
            sc.set(f'spark.sql.catalog.{name}.auth-type', auth_type)
            if auth_type == 'BEARER':
                token = self.connection.extra.get('nessie_token', '')
                sc.set(f'spark.sql.catalog.{name}.auth.token', token)

        elif self.catalog == 'rest':
            sc.set(f'spark.sql.catalog.{name}', 'org.apache.iceberg.spark.SparkCatalog')
            sc.set(f'spark.sql.catalog.{name}.type', 'rest')
            if self.catalog_uri:
                sc.set(f'spark.sql.catalog.{name}.uri', self.catalog_uri)
            if self.catalog_warehouse:
                sc.set(f'spark.sql.catalog.{name}.warehouse', self.catalog_warehouse)
            credential = self.connection.extra.get('rest_credential', None)
            if credential:
                sc.set(f'spark.sql.catalog.{name}.credential', credential)
            token = self.connection.extra.get('rest_token', None)
            if token:
                sc.set(f'spark.sql.catalog.{name}.token', token)
            scope = self.connection.extra.get('rest_scope', None)
            if scope:
                sc.set(f'spark.sql.catalog.{name}.scope', scope)

        elif self.catalog == 'hadoop':
            sc.set(f'spark.sql.catalog.{name}', 'org.apache.iceberg.spark.SparkCatalog')
            sc.set(f'spark.sql.catalog.{name}.type', 'hadoop')
            if self.catalog_warehouse:
                sc.set(f'spark.sql.catalog.{name}.warehouse', self.catalog_warehouse)

        # Note: spark.sql.extensions is a static config and must be set during SparkSession creation
        # Check if extensions are already configured
        try:
            extensions = sc.get('spark.sql.extensions', '')
            if 'IcebergSparkSessionExtensions' not in extensions:
                logger.warning(
                    {
                        'message': 'Iceberg extensions not found in spark.sql.extensions',
                        'hint': 'Add to SparkSession config: spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions',
                    }
                )
        except Exception:
            pass

    @staticmethod
    def _parse_partition_transform(expr: str):
        """Convert a partition spec string into a Spark Column for Iceberg partitionedBy().

        Supported transforms: years(col), months(col), days(col), hours(col),
        bucket(N, col), truncate(N, col), or plain column name.
        """
        import re
        from pyspark.sql.functions import col

        # Spark 4.x moved partition transforms to pyspark.sql.functions.partitioning
        try:
            from pyspark.sql.functions.partitioning import years, months, days, hours
        except ImportError:
            from pyspark.sql.functions import years, months, days, hours

        expr = expr.strip()
        # Match function-style: func(args)
        m = re.match(r'^(\w+)\((.+)\)$', expr)
        if m:
            func_name = m.group(1).lower()
            args_str = m.group(2).strip()

            transform_map = {
                'year': years,
                'years': years,
                'month': months,
                'months': months,
                'day': days,
                'days': days,
                'hour': hours,
                'hours': hours,
            }

            if func_name in transform_map:
                return transform_map[func_name](col(args_str))

            # bucket and truncate may not be available in all PySpark versions
            if func_name in ('bucket', 'truncate'):
                parts = [p.strip() for p in args_str.split(',')]
                if len(parts) == 2:
                    n = int(parts[0])
                    col_name = parts[1]
                    try:
                        if func_name == 'bucket':
                            from pyspark.sql.functions import bucket as _bucket

                            return _bucket(n, col(col_name))
                        else:
                            from pyspark.sql.functions import truncate as _truncate

                            return _truncate(col(col_name), n)
                    except ImportError:
                        raise ImportError(
                            f"PySpark version does not support '{func_name}' partition transform. "
                            f'Upgrade to PySpark 3.4+ or use a different partition strategy.'
                        )

            raise ValueError(f'Unsupported partition transform: {expr}')

        # Plain column name
        return col(expr)

    @staticmethod
    def _iceberg_table_exists(spark, full_table: str) -> bool:
        """Check if an Iceberg table already exists in the catalog."""
        log_level = spark.sparkContext.getConf().get('spark.log.level', 'WARN')
        try:
            spark.sparkContext.setLogLevel('OFF')
            spark.table(full_table).schema
            return True
        except Exception:
            return False
        finally:
            spark.sparkContext.setLogLevel(log_level)

    # Iceberg-supported type promotions (safe widening)
    _TYPE_PROMOTIONS = {
        ('int', 'bigint'),
        ('float', 'double'),
        ('decimal(10,0)', 'decimal(20,0)'),  # example; checked via startswith below
    }

    _PROTECTED_COLUMNS = {'etl_time', 'mkpipe_id'}

    @staticmethod
    def _is_safe_promotion(old_type: str, new_type: str) -> bool:
        """Check if type change is a safe Iceberg promotion."""
        old_t = old_type.lower().strip()
        new_t = new_type.lower().strip()
        if old_t == new_t:
            return True
        # int -> bigint
        if old_t == 'int' and new_t == 'bigint':
            return True
        # float -> double
        if old_t == 'float' and new_t == 'double':
            return True
        # decimal widening (higher precision/scale)
        if old_t.startswith('decimal') and new_t.startswith('decimal'):
            return True
        # string accepts anything
        if new_t == 'string':
            return True
        return False

    def _evolve_iceberg_schema(
        self,
        spark,
        full_table: str,
        df,
        mode: str,
        protected_extra: set[str] | None = None,
    ) -> None:
        """Handle schema evolution for an existing Iceberg table.

        Modes:
            merge   – add new columns, widen compatible types, drop removed columns
            replace – skip evolution, table will be recreated
            strict  – raise error on any schema mismatch
        """
        if mode == 'replace':
            return

        existing = spark.table(full_table).schema
        incoming = df.schema

        existing_map = {f.name.lower(): f for f in existing.fields}
        incoming_map = {f.name.lower(): f for f in incoming.fields}

        existing_names = set(existing_map.keys())
        incoming_names = set(incoming_map.keys())

        new_columns = incoming_names - existing_names
        dropped_columns = existing_names - incoming_names

        # Build protected set: metadata + partition + sort columns
        protected = {c.lower() for c in self._PROTECTED_COLUMNS}
        if protected_extra:
            protected |= {c.lower() for c in protected_extra}

        # Remove protected columns from drop candidates
        droppable = dropped_columns - protected

        # Check type changes on common columns
        type_changes = {}
        for col_name in existing_names & incoming_names:
            old_type = existing_map[col_name].dataType.simpleString()
            new_type = incoming_map[col_name].dataType.simpleString()
            if old_type != new_type:
                type_changes[col_name] = (old_type, new_type)

        if mode == 'strict':
            issues = []
            if new_columns:
                issues.append(f'new columns: {new_columns}')
            if droppable:
                issues.append(f'dropped columns: {droppable}')
            if type_changes:
                issues.append(f'type changes: {type_changes}')
            if issues:
                raise ConfigError(
                    f"Schema mismatch on '{full_table}': {'; '.join(issues)}. "
                    f"Set iceberg_schema_evolution='merge' to auto-evolve."
                )

        # Add new columns
        if new_columns:
            for col_name in new_columns:
                field = incoming_map[col_name]
                try:
                    spark.sql(
                        f'ALTER TABLE {full_table} ADD COLUMNS '
                        f'({field.name} {field.dataType.simpleString()})'
                    )
                    logger.info(
                        {
                            'table': full_table,
                            'schema_evolution': 'add_column',
                            'column': field.name,
                            'type': field.dataType.simpleString(),
                        }
                    )
                except Exception:
                    logger.warning(
                        {
                            'table': full_table,
                            'schema_evolution': 'add_column_skipped',
                            'column': field.name,
                            'reason': 'ALTER TABLE failed, Iceberg SQL extensions may be incompatible',
                        }
                    )

        # Widen types where safe
        for col_name, (old_type, new_type) in type_changes.items():
            if self._is_safe_promotion(old_type, new_type):
                try:
                    spark.sql(
                        f'ALTER TABLE {full_table} ALTER COLUMN '
                        f'{col_name} TYPE {new_type}'
                    )
                    logger.info(
                        {
                            'table': full_table,
                            'schema_evolution': 'type_widen',
                            'column': col_name,
                            'old_type': old_type,
                            'new_type': new_type,
                        }
                    )
                except Exception:
                    logger.warning(
                        {
                            'table': full_table,
                            'schema_evolution': 'type_widen_skipped',
                            'column': col_name,
                            'old_type': old_type,
                            'new_type': new_type,
                            'reason': 'type change not supported by catalog',
                        }
                    )
            else:
                logger.warning(
                    {
                        'table': full_table,
                        'schema_evolution': 'type_change_incompatible',
                        'column': col_name,
                        'old_type': old_type,
                        'new_type': new_type,
                        'reason': 'not a safe promotion, column cast may be needed',
                    }
                )

        # Drop removed columns (only non-protected)
        if droppable and mode == 'merge':
            for col_name in droppable:
                try:
                    spark.sql(f'ALTER TABLE {full_table} DROP COLUMN {col_name}')
                    logger.info(
                        {
                            'table': full_table,
                            'schema_evolution': 'drop_column',
                            'column': col_name,
                        }
                    )
                except Exception:
                    logger.warning(
                        {
                            'table': full_table,
                            'schema_evolution': 'drop_column_skipped',
                            'column': col_name,
                            'reason': 'column drop not supported or failed',
                        }
                    )

    @staticmethod
    def _align_df_to_table(spark, df, full_table: str):
        """Align DataFrame columns to match the existing Iceberg table schema.

        - Reorder columns to match table order
        - Add missing columns as NULL (columns that exist in table but not in df)
        - Drop extra columns not in table schema
        This ensures append never fails due to column order/count mismatch.
        """
        from pyspark.sql.functions import lit

        table_schema = spark.table(full_table).schema
        table_col_names = [f.name.lower() for f in table_schema.fields]
        df_col_names = {f.name.lower(): f.name for f in df.schema.fields}

        select_cols = []
        for tf in table_schema.fields:
            col_lower = tf.name.lower()
            if col_lower in df_col_names:
                select_cols.append(
                    df[df_col_names[col_lower]].cast(tf.dataType).alias(tf.name)
                )
            else:
                select_cols.append(lit(None).cast(tf.dataType).alias(tf.name))

        return df.select(*select_cols)

    def _apply_iceberg_sort_order(
        self, spark, full_table: str, sort_by: list[str]
    ) -> None:
        """Set write sort order on an Iceberg table via ALTER TABLE."""
        sort_spec = ', '.join(s.strip() for s in sort_by)
        try:
            spark.sql(f'ALTER TABLE {full_table} WRITE ORDERED BY {sort_spec}')
            logger.info(
                {
                    'table': full_table,
                    'iceberg_sort_order': sort_spec,
                }
            )
        except Exception as e:
            logger.warning(
                {
                    'table': full_table,
                    'iceberg_sort_order_skipped': sort_spec,
                    'reason': str(e)[:200],
                    'hint': 'Iceberg SQL extensions may be incompatible with your Spark version',
                }
            )

    def _write_iceberg(
        self, spark, df, full_table: str, write_mode: str, table: TableConfig
    ) -> None:
        """Write DataFrame to an Iceberg table with partition, properties, sort order, and schema evolution."""
        partition_by = table.iceberg_partition_by
        sort_by = table.iceberg_sort_by
        properties = table.iceberg_properties
        schema_evolution = table.iceberg_schema_evolution
        table_exists = self._iceberg_table_exists(spark, full_table)

        # Collect protected column names (partition + sort) so they won't be dropped
        protected_extra: set[str] = set()
        if partition_by:
            for spec in partition_by:
                # Extract column name from transform: "day(order_date)" -> "order_date"
                s = spec.strip()
                if '(' in s and s.endswith(')'):
                    col_part = s[s.index('(') + 1 : -1]
                    # Handle bucket(16, id) -> id
                    parts = [p.strip() for p in col_part.split(',')]
                    protected_extra.add(parts[-1].lower())
                else:
                    protected_extra.add(s.lower())
        if sort_by:
            for s in sort_by:
                protected_extra.add(s.strip().lower())

        if table_exists and schema_evolution != 'replace':
            self._evolve_iceberg_schema(
                spark, full_table, df, schema_evolution, protected_extra=protected_extra
            )

            if sort_by:
                self._apply_iceberg_sort_order(spark, full_table, sort_by)
            else:
                # Remove existing sort order to prevent unwanted pre-sort overhead
                try:
                    spark.sql(f'ALTER TABLE {full_table} WRITE UNORDERED')
                except Exception:
                    pass

            # Apply table properties to existing table via ALTER TABLE
            if properties:
                for key, value in properties.items():
                    try:
                        spark.sql(
                            f"ALTER TABLE {full_table} SET TBLPROPERTIES ('{key}' = '{value}')"
                        )
                    except Exception as e:
                        logger.warning(
                            {
                                'table': full_table,
                                'iceberg_property_skipped': f'{key}={value}',
                                'reason': str(e)[:200],
                            }
                        )

            # Align DataFrame columns to table schema (order, types, missing cols as NULL)
            df = self._align_df_to_table(spark, df, full_table)

            if write_mode == 'overwrite':
                if partition_by:
                    df.writeTo(full_table).overwritePartitions()
                else:
                    df.writeTo(full_table).using('iceberg').createOrReplace()
            else:
                df.writeTo(full_table).append()
        else:
            writer = df.writeTo(full_table).using('iceberg')

            if partition_by:
                partition_cols = [
                    self._parse_partition_transform(p) for p in partition_by
                ]
                writer = writer.partitionedBy(*partition_cols)

            for key, value in properties.items():
                writer = writer.tableProperty(key, value)

            if write_mode == 'overwrite':
                writer.createOrReplace()
            else:
                writer.create()

            if sort_by:
                self._apply_iceberg_sort_order(spark, full_table, sort_by)

    def _delta_table_exists(
        self, spark, full_table: str | None, path: str | None
    ) -> bool:
        """Check if a Delta table already exists."""
        try:
            if full_table:
                spark.sql(f'DESCRIBE TABLE {full_table}')
            elif path:
                spark.read.format('delta').load(path).limit(0)
            else:
                return False
            return True
        except Exception:
            return False

    def _write_delta(
        self,
        spark,
        df,
        target_name: str,
        path: str,
        write_mode: str,
        table: TableConfig,
    ) -> None:
        """Write DataFrame to a Delta table with partition, z-order, properties, and schema evolution."""
        partition_by = table.delta_partition_by
        z_order_by = table.delta_z_order_by
        properties = table.delta_properties
        schema_evolution = table.delta_schema_evolution

        full_table = f'{self.catalog_name}.{target_name}' if self.catalog else None
        table_exists = self._delta_table_exists(spark, full_table, path)

        if table_exists and schema_evolution != 'replace':
            # Schema evolution for existing table
            if schema_evolution == 'merge':
                merge_opts = {'mergeSchema': 'true'}
            elif schema_evolution == 'strict':
                merge_opts = {}
            else:
                merge_opts = {}

            if full_table:
                # Align DataFrame to existing table schema
                df = self._align_df_to_table(spark, df, full_table)
                writer = df.writeTo(full_table)
                if write_mode == 'overwrite':
                    writer.using('delta').createOrReplace()
                else:
                    writer.append()
            else:
                writer = df.write.format('delta').mode(write_mode)
                for k, v in merge_opts.items():
                    writer = writer.option(k, v)
                if partition_by:
                    writer = writer.partitionBy(*partition_by)
                writer.save(path)
        else:
            # Create new table
            if full_table:
                writer = df.writeTo(full_table).using('delta')
                if partition_by:
                    writer = writer.partitionedBy(*partition_by)
                for key, value in properties.items():
                    writer = writer.tableProperty(key, value)
                if write_mode == 'overwrite':
                    writer.createOrReplace()
                else:
                    writer.create()
            else:
                writer = df.write.format('delta').mode(write_mode)
                if schema_evolution == 'merge':
                    writer = writer.option('mergeSchema', 'true')
                if partition_by:
                    writer = writer.partitionBy(*partition_by)
                writer.save(path)

        # Set table properties via ALTER TABLE (catalog-based only)
        if full_table and properties and table_exists:
            for key, value in properties.items():
                try:
                    spark.sql(
                        f"ALTER TABLE {full_table} SET TBLPROPERTIES ('{key}' = '{value}')"
                    )
                except Exception:
                    pass

        # Z-Order optimization (runs after write, catalog-based only)
        if z_order_by and full_table:
            z_cols = ', '.join(z_order_by)
            try:
                spark.sql(f'OPTIMIZE {full_table} ZORDER BY ({z_cols})')
                logger.info(
                    {
                        'table': full_table or path,
                        'delta_z_order': z_cols,
                    }
                )
            except Exception:
                logger.warning(
                    {
                        'table': full_table or path,
                        'delta_z_order_skipped': z_cols,
                        'reason': 'OPTIMIZE ZORDER requires Delta catalog table',
                    }
                )

    def load(self, table: TableConfig, data: ExtractResult, spark) -> None:
        target_name = table.target_name
        write_mode = data.write_mode
        df = data.df

        if df is None:
            logger.info(
                {'table': target_name, 'status': 'skipped', 'reason': 'no data'}
            )
            return

        df = add_etl_columns(df, datetime.now(), dedup_columns=table.dedup_columns)

        if table.write_partitions:
            df = df.coalesce(table.write_partitions)

        logger.info(
            {
                'table': target_name,
                'status': 'loading',
                'storage': self.storage,
                'format': self.format,
                'write_mode': write_mode,
                'catalog': self.catalog,
            }
        )

        self._configure_storage(spark)
        path = self._resolve_path(target_name)

        if self.format == 'iceberg':
            if self.catalog:
                self._configure_catalog(spark)
            full_table = f'{self.catalog_name}.{self.catalog_database}.{target_name}'
            self._write_iceberg(spark, df, full_table, write_mode, table)
        elif self.format == 'delta':
            if self.catalog:
                self._configure_delta_catalog(spark)
            self._write_delta(spark, df, target_name, path, write_mode, table)
        else:
            writer = df.write.format(self.format).mode(write_mode)
            if self.format == 'csv':
                writer = writer.option('header', 'true')
            writer.save(path)

        df.unpersist()
        gc.collect()

        logger.info({'table': target_name, 'status': 'loaded'})
