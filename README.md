# mkpipe-loader-file

Unified file-based loader for mkpipe. Writes data to local or cloud storage in multiple formats.

## Installation

```bash
pip install mkpipe-loader-file
```

## Supported Storage

| Storage | Scheme | Description |
|---------|--------|-------------|
| Local | (no prefix) | Local filesystem paths |
| S3 | `s3a://` | Amazon S3 / S3-compatible |
| GCS | `gs://` | Google Cloud Storage |
| ADLS | `abfss://` | Azure Data Lake Storage |
| HDFS | `hdfs://` | Hadoop Distributed File System |

## Supported Formats

`parquet`, `csv`, `json`, `orc`, `avro`, `iceberg`, `delta`

## Connection Configuration

Connection is configured in the `connections` section of `mkpipe_project.yaml`:

```yaml
connections:
  target:
    variant: file
    extra:
      storage: local        # local | s3 | gcs | adls | hdfs
      format: parquet        # parquet | csv | json | orc | avro | iceberg | delta
      path: /data/output     # base path (files written as <path>/<target_name>)
```

### Connection Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `variant` | str | **required** | Must be `file` |
| `extra.storage` | str | `local` | Storage backend: `local`, `s3`, `gcs`, `adls`, `hdfs` |
| `extra.format` | str | `parquet` | File format to write |
| `extra.path` | str | `""` | Base path. Target name is appended: `<path>/<target_name>` |
| `extra.catalog_name` | str | `default` | Catalog name (only for `iceberg` format) |
| `bucket_name` | str | - | S3 bucket name (used when `path` is not set) |
| `s3_prefix` | str | - | S3 key prefix inside the bucket |
| `aws_access_key` | str | - | AWS access key for S3 |
| `aws_secret_key` | str | - | AWS secret key for S3 |
| `region` | str | - | AWS region for S3 |
| `credentials_file` | str | - | Service account JSON key file path for GCS |

### Path Resolution

The loader resolves output paths in this order:
1. If `extra.path` is set: `<path>/<target_name>`
2. If `storage=s3` and `bucket_name` is set: `s3a://<bucket_name>/<s3_prefix>/<target_name>`
3. Otherwise: `<target_name>` as-is

## Table Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `dedup_columns` | list | - | Columns used to generate `mkpipe_id` (xxhash64) for deduplication |
| `write_partitions` | int | - | Number of output partitions (`df.coalesce(N)` before write) |

The loader automatically adds two metadata columns to every written dataset:
- `etl_time` - timestamp of when the load was executed
- `mkpipe_id` - xxhash64 hash of `dedup_columns` (if configured)

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
    target: file_target
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

### Apache Iceberg

```yaml
connections:
  file_target:
    variant: file
    extra:
      format: iceberg
      catalog_name: my_catalog

pipelines:
  - name: to_iceberg
    source: my_source
    target: file_target
    tables:
      - name: orders
        target_name: warehouse.orders
```

- `overwrite` mode: uses `writeTo(...).createOrReplace()`
- `append` mode: uses `writeTo(...).append()`

### Delta Lake

```yaml
connections:
  file_target:
    variant: file
    extra:
      storage: s3
      format: delta
      path: s3a://my-bucket/delta-tables
```

## Write Modes

Write mode is determined automatically by the extractor:

- **overwrite** - full load, replaces existing data
- **append** - incremental load, adds new data

## CSV Options

When `format: csv`, the loader automatically applies:
- `header: true` (writes column names as the first row)

## Deduplication

If `dedup_columns` is configured on the table, the loader generates an `mkpipe_id` column using xxhash64 of the specified columns. This can be used for downstream deduplication.

```yaml
tables:
  - name: events
    target_name: events
    dedup_columns:
      - event_id
      - event_timestamp
```

## Notes

- Environment variables can be used with `${VAR_NAME}` syntax in YAML
- For S3-compatible storage (MinIO, etc.), set the endpoint via Spark config in `settings.spark.extra_config`
- Iceberg format requires a properly configured Spark catalog
- Delta format requires `delta-spark` package in your Spark environment
- The loader calls `df.unpersist()` and `gc.collect()` after writing to free memory
