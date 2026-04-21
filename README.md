# mkpipe-loader-file

Unified file-based loader for mkpipe. Writes data to local or cloud storage in multiple formats including Parquet, CSV, JSON, ORC, Avro, Apache Iceberg, and Delta Lake.

## Installation

```bash
pip install mkpipe-loader-file
```

## Supported Storage

| Storage | Value | Scheme | Description |
|---------|-------|--------|-------------|
| Local filesystem | `local` | (no prefix) | Local or network-mounted paths |
| Amazon S3 | `s3` | `s3a://` | AWS S3 or S3-compatible (MinIO, etc.) |
| Google Cloud Storage | `gcs` | `gs://` | GCP Cloud Storage |
| Azure Data Lake | `adls` | `abfss://` | Azure Data Lake Storage Gen2 |
| HDFS | `hdfs` | `hdfs://` | Hadoop Distributed File System |

## Supported Formats

| Format | Value | Notes |
|--------|-------|-------|
| Apache Parquet | `parquet` | Default. Columnar, highly recommended for large datasets |
| CSV | `csv` | Writes header row automatically |
| JSON | `json` | One JSON object per line (newline-delimited) |
| ORC | `orc` | Columnar, common in Hive ecosystems |
| Avro | `avro` | Row-based, good for streaming pipelines |
| Apache Iceberg | `iceberg` | Table format with catalog support (Glue, Nessie, REST, Hadoop) |
| Delta Lake | `delta` | Table format with catalog support (HMS, Unity Catalog) |

## Connection Configuration

```yaml
connections:
  target:
    variant: file
    extra:
      storage: local        # local | s3 | gcs | adls | hdfs
      format: parquet       # parquet | csv | json | orc | avro | iceberg | delta
      path: /data/output    # base path — target name appended: <path>/<target_name>
```

### Connection Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `variant` | str | **required** | Must be `file` |
| `extra.storage` | str | `local` | Storage backend: `local` \| `s3` \| `gcs` \| `adls` \| `hdfs` |
| `extra.format` | str | `parquet` | File format: `parquet` \| `csv` \| `json` \| `orc` \| `avro` \| `iceberg` \| `delta` |
| `extra.path` | str | `""` | Base path. Target name appended: `<path>/<target_name>` |
| `extra.catalog` | str | `null` | Catalog type — **Iceberg**: `glue` \| `nessie` \| `rest` \| `hadoop` · **Delta**: `hms` \| `unity` |
| `extra.catalog_name` | str | `default` | Spark catalog identifier — table referenced as `<catalog_name>.<catalog_database>.<table>` |
| `extra.catalog_database` | str | `default` | Database/namespace within the catalog (e.g. Glue database name). Table path on S3: `<warehouse>/<catalog_database>.db/<table>/` |
| `extra.catalog_uri` | str | `null` | Catalog endpoint URI (Nessie: `http://...`, REST: `https://...`, HMS: `thrift://...`) |
| `extra.catalog_warehouse` | str | `null` | Warehouse root path (S3/GCS/HDFS URI or local path) |
| `extra.nessie_ref` | str | `main` | Nessie branch or tag to write to |
| `extra.nessie_auth_type` | str | `NONE` | Nessie auth type: `NONE` \| `BEARER` |
| `extra.nessie_token` | str | `null` | Nessie bearer token |
| `extra.rest_credential` | str | `null` | REST catalog OAuth2 credential `client_id:client_secret` |
| `extra.rest_scope` | str | `null` | REST catalog OAuth2 scope (e.g. `PRINCIPAL_ROLE:my_role`) |
| `extra.unity_token` | str | `null` | Databricks personal access token for Unity Catalog |
| `bucket_name` | str | `null` | S3 bucket name (used when `path` is not set) |
| `s3_prefix` | str | `null` | S3 key prefix inside the bucket |
| `aws_access_key` | str | `null` | AWS access key ID |
| `aws_secret_key` | str | `null` | AWS secret access key |
| `region` | str | `null` | AWS region (S3 and Glue) |
| `credentials_file` | str | `null` | GCS service account JSON key file path |

### Path Resolution

For non-catalog formats (`parquet`, `csv`, `json`, `orc`, `avro`, `delta` without catalog), the loader resolves paths in this order:

1. `extra.path` is set → `<path>/<target_name>`
2. `storage=s3` and `bucket_name` is set → `s3a://<bucket_name>/<s3_prefix>/<target_name>`
3. Otherwise → `<target_name>` as-is

For catalog-based formats (`iceberg` with `catalog`), the path is ignored — the table is referenced as `<catalog_name>.<catalog_database>.<target_name>`. For `delta` with catalog, it is `<catalog_name>.<target_name>`.

## Table Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `name` | str | **required** | Source table name (from extractor) |
| `target_name` | str | **required** | Output path/table name. For catalogs: `<db>.<table>` |
| `dedup_columns` | list | `null` | Columns used to generate `mkpipe_id` (xxhash64) for deduplication |
| `write_partitions` | int | `null` | Coalesce DataFrame to N partitions before writing (`df.coalesce(N)`) |
| `batchsize` | int | `10000` | Batch size hint (used by some downstream connectors) |
| `write_strategy` | str | `null` | `append` or `replace` (upsert/merge not supported) |
| `tags` | list | `[]` | Tags for selective pipeline execution (`mkpipe run --tags ...`) |
| `iceberg_partition_by` | list | `null` | Iceberg partition spec. Supports transforms: `year(col)`, `month(col)`, `day(col)`, `hour(col)`, `bucket(N, col)`, `truncate(N, col)`, or plain column names |
| `iceberg_sort_by` | list | `null` | Write sort order columns. Improves query performance on filtered/joined columns. Applied via `ALTER TABLE ... WRITE ORDERED BY` |
| `iceberg_properties` | dict | `{}` | Iceberg table properties (e.g. `write.format.default`, `write.parquet.compression-codec`) |
| `iceberg_schema_evolution` | str | `merge` | Schema evolution mode: `merge` (add/drop columns), `replace` (recreate table), `strict` (error on mismatch) |
| `delta_partition_by` | list | `null` | Delta partition columns (plain column names only, no transforms) |
| `delta_z_order_by` | list | `null` | Z-Order optimization columns. Runs `OPTIMIZE ... ZORDER BY` after write (catalog-based only) |
| `delta_properties` | dict | `{}` | Delta table properties (e.g. `delta.autoOptimize.optimizeWrite`, `delta.autoOptimize.autoCompact`) |
| `delta_schema_evolution` | str | `merge` | Schema evolution mode: `merge` (mergeSchema), `replace` (recreate table), `strict` (error on mismatch) |

### Metadata Columns

The loader automatically appends two columns to every written dataset:

| Column | Type | Description |
|--------|------|-------------|
| `_ingested_at` | timestamp | Timestamp when the load was executed (configurable via `settings.ingested_at_column`) |
| `mkpipe_id` | bigint | xxhash64 of `dedup_columns` values — used for downstream deduplication (configurable via `settings.ingestion_id_column`) |

The dedup ID column is only populated when `dedup_columns` is configured.

## YAML Examples

### Local Filesystem - Parquet

```yaml
connections:
  my_db:
    variant: postgres
    host: localhost
    port: 5432
    database: mydb
    user: ${DB_USER}
    password: ${DB_PASSWORD}
  file_target:
    variant: file
    extra:
      storage: local
      format: parquet
      path: /data/output

pipelines:
  - name: db_to_file
    source: my_db
    destination: file_target
    tables:
      - name: users
        target_name: users
        write_partitions: 4
```

### Amazon S3 - CSV

```yaml
connections:
  file_target:
    variant: file
    extra:
      storage: s3
      format: csv
    bucket_name: my-output-bucket
    s3_prefix: exports/daily
    aws_access_key: ${AWS_ACCESS_KEY_ID}
    aws_secret_key: ${AWS_SECRET_ACCESS_KEY}
    region: eu-central-1
```

### Google Cloud Storage - JSON

```yaml
connections:
  file_target:
    variant: file
    extra:
      storage: gcs
      format: json
      path: gs://my-bucket/exports
    credentials_file: /secrets/gcp-sa.json
```

### Apache Iceberg — Glue Catalog

```yaml
connections:
  file_target:
    variant: file
    extra:
      storage: s3
      format: iceberg
      catalog: glue
      catalog_name: my_glue
      catalog_database: my_database   # Glue database name (default: "default")
      catalog_warehouse: s3a://my-bucket/warehouse
    aws_access_key: ${AWS_ACCESS_KEY_ID}
    aws_secret_key: ${AWS_SECRET_ACCESS_KEY}
    region: eu-central-1

settings:
  spark:
    extra_config:
      spark.sql.extensions: "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"

pipelines:
  - name: to_glue
    source: my_source
    destination: file_target
    tables:
      - name: public.orders
        target_name: orders
```

The table will be registered in Glue as `my_database.orders` and stored at `s3a://my-bucket/warehouse/my_database.db/orders/`.

- `overwrite` mode: uses `writeTo(...).createOrReplace()` — creates the table if it doesn't exist
- `append` mode: uses `writeTo(...).append()`

> **Important:** `spark.sql.extensions` must be set in `settings.spark.extra_config` — it is a static Spark config and cannot be modified after SparkSession creation.

### Apache Iceberg — Partitioned Table with Properties

```yaml
pipelines:
  - name: to_glue
    source: my_source
    destination: file_target
    tables:
      - name: public.orders
        target_name: orders
        iceberg_partition_by:
          - "day(order_date)"       # daily partition using Iceberg transform
          - region                  # identity partition on region column
        iceberg_sort_by:
          - order_date              # sort within each partition for faster queries
          - customer_id
        iceberg_properties:
          write.format.default: parquet
          write.parquet.compression-codec: zstd
          write.metadata.delete-after-commit.enabled: "true"
          write.metadata.previous-versions-max: "3"
        iceberg_schema_evolution: merge   # merge | replace | strict
```

**Supported partition transforms:**

| Transform | Example | Description |
|-----------|---------|-------------|
| Identity | `region` | Partition by exact column value |
| Year | `year(ts)` | Extract year from timestamp/date |
| Month | `month(ts)` | Extract year-month |
| Day | `day(ts)` | Extract year-month-day |
| Hour | `hour(ts)` | Extract year-month-day-hour |
| Bucket | `bucket(16, id)` | Hash partition into N buckets |
| Truncate | `truncate(10, name)` | Truncate string/int to width |

**Schema evolution modes:**

| Mode | Behavior |
|------|----------|
| `merge` (default) | Add new columns, drop removed columns automatically |
| `replace` | Drop and recreate the table with the new schema on every overwrite |
| `strict` | Raise an error if the incoming schema differs from the existing table |

**Common Iceberg table properties:**

| Property | Description |
|----------|-------------|
| `write.format.default` | File format: `parquet` (default), `orc`, `avro` |
| `write.parquet.compression-codec` | Compression: `zstd`, `snappy`, `gzip`, `lz4` |
| `write.target-file-size-bytes` | Target file size (default: 536870912 = 512MB) |
| `write.metadata.delete-after-commit.enabled` | Auto-clean old metadata files |
| `write.metadata.previous-versions-max` | Number of metadata versions to keep |

### Apache Iceberg — Nessie Catalog

```yaml
connections:
  file_target:
    variant: file
    extra:
      format: iceberg
      catalog: nessie
      catalog_name: nessie
      catalog_uri: http://nessie-server:19120/api/v1
      catalog_warehouse: s3a://my-bucket/warehouse
      nessie_ref: main              # branch/tag to write to
      nessie_auth_type: BEARER      # NONE | BEARER
      nessie_token: ${NESSIE_TOKEN}
    aws_access_key: ${AWS_ACCESS_KEY_ID}
    aws_secret_key: ${AWS_SECRET_ACCESS_KEY}
    region: eu-central-1

pipelines:
  - name: to_nessie
    source: my_source
    destination: file_target
    tables:
      - name: public.orders
        target_name: my_db.orders
```

### Apache Iceberg — REST Catalog (Polaris / Unity Catalog / custom)

```yaml
connections:
  file_target:
    variant: file
    extra:
      format: iceberg
      catalog: rest
      catalog_name: polaris
      catalog_uri: https://polaris.example.com/api/catalog
      catalog_warehouse: my_warehouse
      rest_credential: "client_id:client_secret"  # OAuth2 client credentials
      rest_scope: PRINCIPAL_ROLE:my_role
    aws_access_key: ${AWS_ACCESS_KEY_ID}
    aws_secret_key: ${AWS_SECRET_ACCESS_KEY}

pipelines:
  - name: to_polaris
    source: my_source
    destination: file_target
    tables:
      - name: public.orders
        target_name: my_namespace.orders
```

### Apache Iceberg — Hadoop Catalog (local / HDFS)

```yaml
connections:
  file_target:
    variant: file
    extra:
      format: iceberg
      catalog: hadoop
      catalog_name: local
      catalog_warehouse: /data/iceberg-warehouse

pipelines:
  - name: to_hadoop_catalog
    source: my_source
    destination: file_target
    tables:
      - name: public.orders
        target_name: my_db.orders
```

### Delta Lake — path-based (no catalog)

```yaml
connections:
  file_target:
    variant: file
    extra:
      storage: s3
      format: delta
      path: s3a://my-bucket/delta-tables
```

### Delta Lake — Hive Metastore (HMS)

```yaml
connections:
  file_target:
    variant: file
    extra:
      format: delta
      catalog: hms
      catalog_name: spark_catalog
      catalog_uri: thrift://hive-metastore:9083

pipelines:
  - name: to_hms_delta
    source: my_source
    destination: file_target
    tables:
      - name: public.orders
        target_name: my_db.orders   # <hive_database>.<table>
```

### Delta Lake — Unity Catalog

```yaml
connections:
  file_target:
    variant: file
    extra:
      format: delta
      catalog: unity
      catalog_name: my_unity_catalog
      catalog_uri: https://my-workspace.azuredatabricks.net
      unity_token: ${DATABRICKS_TOKEN}

pipelines:
  - name: to_unity
    source: my_source
    destination: file_target
    tables:
      - name: public.orders
        target_name: my_schema.orders   # <schema>.<table> within Unity Catalog
```

- `overwrite` mode: uses `writeTo(...).using('delta').createOrReplace()`
- `append` mode: uses `writeTo(...).using('delta').append()`

> **Important:** `spark.sql.extensions` must be set in `settings.spark.extra_config` — it is a static Spark config and cannot be modified after SparkSession creation.

### Delta Lake — Partitioned Table with Z-Order

```yaml
pipelines:
  - name: to_hms_delta
    source: my_source
    destination: file_target
    tables:
      - name: public.orders
        target_name: my_db.orders
        delta_partition_by:
          - order_date               # plain column names only (no transforms)
        delta_z_order_by:
          - customer_id              # OPTIMIZE ... ZORDER BY (catalog-based only)
          - region
        delta_properties:
          delta.autoOptimize.optimizeWrite: "true"
          delta.autoOptimize.autoCompact: "true"
          delta.logRetentionDuration: "interval 30 days"
          delta.deletedFileRetentionDuration: "interval 7 days"
        delta_schema_evolution: merge   # merge | replace | strict
```

**Delta vs Iceberg partition differences:**

| Feature | Iceberg | Delta |
|---------|---------|-------|
| Partition transforms | `year()`, `month()`, `day()`, `hour()`, `bucket()`, `truncate()` | Plain column names only |
| Sort optimization | `WRITE ORDERED BY` (built-in) | `OPTIMIZE ... ZORDER BY` (separate command) |
| Schema evolution | `ALTER TABLE ADD/DROP/ALTER COLUMN` | `mergeSchema` option |

**Common Delta table properties:**

| Property | Description |
|----------|-------------|
| `delta.autoOptimize.optimizeWrite` | Auto-optimize file sizes during write |
| `delta.autoOptimize.autoCompact` | Auto-compact small files after write |
| `delta.logRetentionDuration` | How long to keep transaction log (default: 30 days) |
| `delta.deletedFileRetentionDuration` | How long to keep deleted data files (default: 7 days) |
| `delta.dataSkippingNumIndexedCols` | Number of columns to collect stats for (default: 32) |

## Write Strategy

Control how data is written to file targets:

```yaml
      - name: public.events
        target_name: events
        write_strategy: append       # append | replace
```

| Strategy | Behavior |
|---|---|
| `append` | Add new rows without touching existing data (default for incremental) |
| `replace` | Overwrite all existing data at the path/table (default for full) |

> **Note:** File loader does not support `upsert` or `merge` strategies. For Iceberg/Delta tables that need merge semantics, consider using a SQL-based loader or a post-load transformation.

For catalog-based Iceberg/Delta tables:
- `replace` → `writeTo(...).createOrReplace()` — creates table if not exists
- `append` → `writeTo(...).append()`

For path-based formats:
- `replace` / `append` → passed directly to `df.write.mode(...)`

## Performance

- **`write_partitions`** — controls output file count. Use `df.coalesce(N)` semantics: reduces partitions without a full shuffle. Recommended for avoiding too many small files on S3/GCS.
- For very large datasets, combine with Spark config `spark.sql.shuffle.partitions` in `settings.spark.extra_config`.

```yaml
tables:
  - name: events
    target_name: events
    write_partitions: 8   # write 8 output files
```

## Notes

- Environment variables: use `${VAR_NAME}` syntax in YAML
- For S3-compatible storage (MinIO, Ceph, etc.), configure the endpoint in `settings.spark.extra_config`:
  ```yaml
  settings:
    spark:
      extra_config:
        spark.hadoop.fs.s3a.endpoint: http://minio:9000
        spark.hadoop.fs.s3a.path.style.access: "true"
  ```
- **Iceberg requires `spark.sql.extensions`** to be set at SparkSession creation time. Add this to your `settings.spark.extra_config`:
  ```yaml
  settings:
    spark:
      extra_config:
        spark.sql.extensions: "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
  ```
  This is a static Spark config and **cannot** be set after the session is created.
- **Delta Lake also requires `spark.sql.extensions`** for catalog-based usage. Add to `settings.spark.extra_config`:
  ```yaml
  settings:
    spark:
      extra_config:
        spark.sql.extensions: "io.delta.sql.DeltaSparkSessionExtension"
  ```
- The loader calls `df.unpersist()` and `gc.collect()` after each table write to free memory
