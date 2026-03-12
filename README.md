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
| `extra.catalog_name` | str | `default` | Spark catalog identifier — table referenced as `<catalog_name>.<db>.<table>` |
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

For catalog-based formats (`iceberg` or `delta` with `catalog`), the path is ignored — the table is referenced as `<catalog_name>.<target_name>`.

## Table Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `name` | str | **required** | Source table name (from extractor) |
| `target_name` | str | **required** | Output path/table name. For catalogs: `<db>.<table>` |
| `dedup_columns` | list | `null` | Columns used to generate `mkpipe_id` (xxhash64) for deduplication |
| `write_partitions` | int | `null` | Coalesce DataFrame to N partitions before writing (`df.coalesce(N)`) |
| `batchsize` | int | `10000` | Batch size hint (used by some downstream connectors) |
| `tags` | list | `[]` | Tags for selective pipeline execution (`mkpipe run --tags ...`) |

### Metadata Columns

The loader automatically appends two columns to every written dataset:

| Column | Type | Description |
|--------|------|-------------|
| `etl_time` | timestamp | Timestamp when the load was executed |
| `mkpipe_id` | bigint | xxhash64 of `dedup_columns` values — used for downstream deduplication |

`mkpipe_id` is only populated when `dedup_columns` is configured.

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
      format: iceberg
      catalog: glue
      catalog_name: my_glue
      catalog_warehouse: s3a://my-bucket/warehouse
    aws_access_key: ${AWS_ACCESS_KEY_ID}
    aws_secret_key: ${AWS_SECRET_ACCESS_KEY}
    region: eu-central-1

pipelines:
  - name: to_glue
    source: my_source
    destination: file_target
    tables:
      - name: public.orders
        target_name: my_db.orders    # <glue_database>.<table>
```

- `overwrite` mode: uses `writeTo(...).createOrReplace()` — creates the table if it doesn't exist
- `append` mode: uses `writeTo(...).append()`

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

## Write Modes

Write mode is determined automatically by the extractor and passed to the loader:

| Mode | Trigger | Behavior |
|------|---------|----------|
| `overwrite` | Full load (first run or `replication_method: full`) | Replaces all existing data at the path/table |
| `append` | Incremental load (subsequent runs) | Adds new rows without touching existing data |

For catalog-based Iceberg/Delta tables:
- `overwrite` → `writeTo(...).createOrReplace()` — creates table if not exists
- `append` → `writeTo(...).append()`

For path-based formats:
- `overwrite` / `append` → passed directly to `df.write.mode(...)`

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
- Iceberg and Delta catalog extensions (`spark.sql.extensions`) are set dynamically at runtime — if your Spark session is pre-created with conflicting config, set them statically in `settings.spark.extra_config` instead
- The loader calls `df.unpersist()` and `gc.collect()` after each table write to free memory
