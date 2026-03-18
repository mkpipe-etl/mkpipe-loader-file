from setuptools import setup, find_packages

setup(
    name='mkpipe-loader-file',
    version='0.4.4',
    license='Apache License 2.0',
    packages=find_packages(),
    install_requires=['mkpipe'],
    include_package_data=True,
    entry_points={
        'mkpipe.loaders': [
            'file = mkpipe_loader_file:FileLoader',
        ],
    },
    description='File-based loader for mkpipe (S3, ADLS, GCS, HDFS, local). Supports parquet, csv, json, orc, avro, iceberg, delta.',
    author='Metin Karakus',
    author_email='metin_karakus@yahoo.com',
    python_requires='>=3.9',
)
