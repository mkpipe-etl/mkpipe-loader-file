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
                f"Unsupported format: '{self.format}'. "
                f"Supported: {SUPPORTED_FORMATS}"
            )
        if self.catalog and self.catalog not in SUPPORTED_CATALOGS:
            raise ConfigError(
                f"Unsupported catalog: '{self.catalog}'. "
                f"Supported: {SUPPORTED_CATALOGS}"
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
                hadoop.set('google.cloud.auth.service.account.json.keyfile', self.credentials_file)

    def _configure_delta_catalog(self, spark):
        """Register a Delta catalog (Unity Catalog or Hive Metastore) in the active Spark session."""
        sc = spark.conf
        name = self.catalog_name

        sc.set('spark.sql.extensions',
               'io.delta.sql.DeltaSparkSessionExtension')
        sc.set('spark.sql.catalog.spark_catalog',
               'org.apache.spark.sql.delta.catalog.DeltaCatalog')

        if self.catalog == 'unity':
            sc.set(f'spark.sql.catalog.{name}', 'com.databricks.spark.sql.catalog.UnitySessionCatalog')
            if self.catalog_uri:
                sc.set('spark.databricks.unityCatalog.enabled', 'true')
                sc.set('spark.databricks.unityCatalog.host', self.catalog_uri)
            token = self.connection.extra.get('unity_token', None)
            if token:
                sc.set('spark.databricks.unityCatalog.token', token)

        elif self.catalog == 'hms':
            sc.set(f'spark.sql.catalog.{name}', 'org.apache.spark.sql.delta.catalog.DeltaCatalog')
            if self.catalog_uri:
                sc.set('spark.hadoop.hive.metastore.uris', self.catalog_uri)

    def _configure_catalog(self, spark):
        """Register the Iceberg catalog in the active Spark session."""
        sc = spark.conf
        name = self.catalog_name

        if self.catalog == 'glue':
            sc.set(f'spark.sql.catalog.{name}', 'org.apache.iceberg.spark.SparkCatalog')
            sc.set(f'spark.sql.catalog.{name}.catalog-impl', 'org.apache.iceberg.aws.glue.GlueCatalog')
            sc.set(f'spark.sql.catalog.{name}.io-impl', 'org.apache.iceberg.aws.s3.S3FileIO')
            if self.catalog_warehouse:
                sc.set(f'spark.sql.catalog.{name}.warehouse', self.catalog_warehouse)
            if self.region:
                sc.set(f'spark.sql.catalog.{name}.glue.region', self.region)
                sc.set(f'spark.sql.catalog.{name}.client.region', self.region)
            if self.aws_access_key:
                sc.set(f'spark.sql.catalog.{name}.s3.access-key-id', self.aws_access_key)
                sc.set(f'spark.sql.catalog.{name}.s3.secret-access-key', self.aws_secret_key or '')
                sc.set(f'spark.sql.catalog.{name}.glue.access-key-id', self.aws_access_key)
                sc.set(f'spark.sql.catalog.{name}.glue.secret-access-key', self.aws_secret_key or '')

        elif self.catalog == 'nessie':
            sc.set(f'spark.sql.catalog.{name}', 'org.apache.iceberg.spark.SparkCatalog')
            sc.set(f'spark.sql.catalog.{name}.catalog-impl', 'org.apache.iceberg.nessie.NessieCatalog')
            sc.set(f'spark.sql.catalog.{name}.io-impl', 'org.apache.iceberg.aws.s3.S3FileIO')
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
                logger.warning({
                    'message': 'Iceberg extensions not found in spark.sql.extensions',
                    'hint': 'Add to SparkSession config: spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions'
                })
        except Exception:
            pass

    def load(self, table: TableConfig, data: ExtractResult, spark) -> None:
        target_name = table.target_name
        write_mode = data.write_mode
        df = data.df

        if df is None:
            logger.info({'table': target_name, 'status': 'skipped', 'reason': 'no data'})
            return

        df = add_etl_columns(df, datetime.now(), dedup_columns=table.dedup_columns)

        if table.write_partitions:
            df = df.coalesce(table.write_partitions)

        logger.info({
            'table': target_name,
            'status': 'loading',
            'storage': self.storage,
            'format': self.format,
            'write_mode': write_mode,
            'catalog': self.catalog,
        })

        self._configure_storage(spark)
        path = self._resolve_path(target_name)

        if self.format == 'iceberg':
            if self.catalog:
                self._configure_catalog(spark)
            full_table = f'{self.catalog_name}.{self.catalog_database}.{target_name}'
            if write_mode == 'overwrite':
                df.writeTo(full_table).using('iceberg').createOrReplace()
            else:
                df.writeTo(full_table).using('iceberg').append()
        elif self.format == 'delta':
            if self.catalog:
                self._configure_delta_catalog(spark)
                full_table = f'{self.catalog_name}.{target_name}'
                if write_mode == 'overwrite':
                    df.writeTo(full_table).using('delta').createOrReplace()
                else:
                    df.writeTo(full_table).using('delta').append()
            else:
                df.write.format('delta').mode(write_mode).save(path)
        else:
            writer = df.write.format(self.format).mode(write_mode)
            if self.format == 'csv':
                writer = writer.option('header', 'true')
            writer.save(path)

        df.unpersist()
        gc.collect()

        logger.info({'table': target_name, 'status': 'loaded'})
