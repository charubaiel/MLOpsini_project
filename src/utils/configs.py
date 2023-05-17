from dataclasses import dataclass
from io import BytesIO, StringIO
from pathlib import Path
import os

import cloudscraper
import duckdb
import pandas as pd
from dagster import ConfigurableResource, Config
import boto3

ROOT = Path(__file__).parent

class ConfigMLFlow(Config):

    MLFLOW_URI: str
    MLFLOW_PORT: str


def init_mlflow():
    my_config = {
        'endpoint_url': 'http://s3:9000',
        'service_name': 's3',
    }
    s3 = boto3.client(**my_config)

    mlflow_s3_bucket = os.getenv('AWS_BUCKET_NAME')
    try:
        s3.list_objects(Bucket=mlflow_s3_bucket)
    except Exception:
        s3.create_bucket(Bucket=mlflow_s3_bucket)


@dataclass
class DatabaseConnection:
    connection_path: str

    def __post_init__(self):
        self.connection = duckdb.connect(self.connection_path)

    def query(self, SQL):
        return self.connection.execute(SQL)

    def close(self):
        self.connection.close()

    def append_df(self, df: pd.DataFrame, table_name: str) -> None:
        self.connection.execute(f'''
            CREATE TABLE IF NOT EXISTS {table_name} as select * from df TABLESAMPLE 0;
            INSERT INTO {table_name}({",".join(df.columns)}) select * from df;
            ''')


@dataclass
class LocalDirConnection:

    path: str = f'{ROOT.parent.parent}/data'

    def save_file(self, bucket: str, name: str, file: BytesIO) -> None:

        filepath = Path(f'{self.path}/{bucket}/{name}')
        if not filepath.parent.exists():
            filepath.parent.mkdir(parents=True, exist_ok=True)

        filepath.write_bytes(file)

    def get_filenames(self, bucket: str) -> list:

        result = []
        files_ = Path(f'{self.path}/{bucket}').glob('*')

        for file in files_:
            result.append(file)
        return result

    def get_data(self, filename: str) -> str:

        return Path(f'{filename}').read_text(encoding='utf-8')

    def remove_data(self, filename: str) -> None:
        Path(filename).unlink()


@dataclass
class s3Connection:

    def __post_init__(self):
        # Let's use Amazon S3
        my_config = {
            'AWS_ACCESS_KEY_ID': os.getenv('AWS_ACCESS_KEY_ID'),
            'AWS_SECRET_ACCESS_KEY': os.getenv('AWS_SECRET_ACCESS_KEY')
        }

        self.client = boto3.client('s3', config=my_config)

        if 'raw' not in self.client.list_buckets():
            self.client.create_bucket(Bucket='raw')

    def save_file(self, bucket: str, name: str, file: BytesIO) -> None:

        if bucket not in self.client.list_buckets():
            self.client.create_bucket(Bucket=bucket)
        try:
            self.client.upload_fileobj(file, bucket, name)
            return True
        except Exception:
            return False

    def get_filenames(self, bucket: str) -> list:

        bucket = self.client.Bucket(bucket)
        result = [i for i in bucket.objects.all()]

        return result

    def get_data(self, bucket: str, filename: str) -> str:

        result = StringIO()
        self.client.download_fileobj(bucket, filename, result)

        return result

    def remove_data(self, bucket: str, filename: str) -> None:
        self.client.delete_object(Bucket=bucket, Key=filename)
        return True


@dataclass
class SimpleParser:

    browser: str = 'chrome'
    platform: str = 'windows'

    def __post_init__(self):
        self.parser = cloudscraper.create_scraper(browser={
            'browser': self.browser,
            'platform': self.platform,
            'desktop': True
        })

    def get(self, url):
        response = self.parser.get(url)
        assert response.status_code == 200, f'Bad Response : {response.status_code}'
        self.html_data = response.text
        return self.html_data

    def close(self):
        self.parser.close()


class ParserResource(ConfigurableResource):

    browser: str = 'chrome'
    platform: str = 'windows'

    def get_client(self):
        return SimpleParser(self.browser, self.platform)


class DatabaseResource(ConfigurableResource):

    connection_path: str

    def get_client(self):
        return DatabaseConnection(self.connection_path)


class S3Resource(ConfigurableResource):

    path: str = f'{ROOT.parent.parent}/data'

    def get_client(self):
        return LocalDirConnection(self.path)


init_mlflow()
