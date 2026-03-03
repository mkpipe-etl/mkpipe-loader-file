import gc
from datetime import datetime

from mkpipe.exceptions import ConfigError
from mkpipe.spark.base import BaseLoader
from mkpipe.spark.columns import add_etl_columns
from mkpipe.models import ConnectionConfig, ExtractResult, TableConfig
from mkpipe.utils import get_logger

JAR_PACKAGES = [
    'org.apache.hadoop:hadoop-aws:3.3.4',
    'com.amazonaws:aws-java-sdk-bundle:1.12.262',
    'com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.22',
    'org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.7.1',
    'io.delta:delta-spark_2.12:3.2.1',
]

logger = get_logger(__name__)

SUPPORTED_FORMATS = ('parquet', 'csv', 'json', 'orc', 'avro', 'iceberg', 'delta')


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

        if self.format not in SUPPORTED_FORMATS:
            raise ConfigError(
                f"Unsupported format: '{self.format}'. "
                f"Supported: {SUPPORTED_FORMATS}"
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
        elif self.storage == 'gcs':
            if self.credentials_file:
                hadoop.set('google.cloud.auth.service.account.json.keyfile', self.credentials_file)

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
        })

        self._configure_storage(spark)
        path = self._resolve_path(target_name)

        if self.format == 'iceberg':
            catalog_name = self.connection.extra.get('catalog_name', 'default')
            full_table = f'{catalog_name}.{target_name}'
            if write_mode == 'overwrite':
                df.writeTo(full_table).using('iceberg').createOrReplace()
            else:
                df.writeTo(full_table).using('iceberg').append()
        elif self.format == 'delta':
            df.write.format('delta').mode(write_mode).save(path)
        else:
            writer = df.write.format(self.format).mode(write_mode)
            if self.format == 'csv':
                writer = writer.option('header', 'true')
            writer.save(path)

        df.unpersist()
        gc.collect()

        logger.info({'table': target_name, 'status': 'loaded'})
