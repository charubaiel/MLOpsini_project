import duckdb
import pandas as pd
from dataclasses import dataclass
from dagster import resource
from contextlib import contextmanager
import cloudscraper
from pathlib import Path
from io import BytesIO
ROOT = Path(__file__).parent


@dataclass
class DatabaseConnection:
    connection_path: str 
    
    def __post_init__(self):
        self.connection = duckdb.connect(self.connection_path)

    def query(self,SQL):
        return self.connection.execute(SQL)
    
    def close(self):
        self.connection.close()

    def append_df(self,
                  df:pd.DataFrame,
                  table_name:str) -> None:
        self.connection.execute(f'''
            CREATE TABLE IF NOT EXISTS {table_name} as select * from df TABLESAMPLE 0;
            INSERT INTO {table_name}({",".join(df.columns)}) select * from df;
            ''')



@dataclass
class S3Connection:

    path: str = f'{ROOT.parent.parent}/data'

    def __post_init__(self):
        self.s3 = 'future_bucket'

    def save_file(self,
                    bucket:str,
                    name:str,
                    file:BytesIO) -> None:
        
        filepath = Path(f'{self.path}/{bucket}/{name}')
        if not filepath.parent.exists():
            filepath.parent.mkdir(parents=True, exist_ok=True)

        with filepath.open("wb", encoding ="utf-8") as f:
            file.seek(0)
            f.write(file.read())


    
    def get_filenames(self,
                        bucket:str) -> list:
        
        result = []
        files_ = Path(f'{self.path}/{bucket}').glob('*')
        
        for file in files_:
                result.append(file)
        return result
    
    def get_data(self,
                filename:str) -> str:
        
        with open(filename) as buffer:
            return buffer.read()
    
    def remove_data(self,
                filename:str) -> None:
        Path(filename).unlink()





@dataclass
class SimpleConnection:

    browser:str = 'chrome'
    platform:str = 'windows'

    def __post_init__(self):
        self.parser = cloudscraper.create_scraper(
                                    browser={
                                        'browser': self.browser,
                                        'platform': self.platform,
                                        'desktop': True
                                    }
                                )

    def get(self,url):
        response = self.parser.get(url)
        self.html_data =  response.text
        return self.html_data
    
    def close(self):
        self.parser.close()





@resource(config_schema={"connection": str})
@contextmanager
def db_resource(init_context):
    try:
        connection = init_context.resource_config["connection"]
        db_conn = DatabaseConnection(connection)
        yield db_conn
    finally :
        db_conn.close()



@resource(config_schema={"auth": str})
def s3_resource(init_context):

    auth = init_context.resource_config["auth"]
    s3_conn = S3Connection()
    yield s3_conn


@resource()
def parser_resource(init_context):
    driver = SimpleConnection()
    try:
        yield driver
    finally :
        driver.close()

