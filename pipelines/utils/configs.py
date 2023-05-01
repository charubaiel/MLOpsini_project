import duckdb
import pandas as pd
from dagster import ConfigurableResource
import cloudscraper
from dataclasses import dataclass
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
class LocalDirConnection:

    path:str = f'{ROOT.parent.parent}/data'

    def save_file(self,
                    bucket:str,
                    name:str,
                    file:BytesIO) -> None:
        
        filepath = Path(f'{self.path}/{bucket}/{name}')
        if not filepath.parent.exists():
            filepath.parent.mkdir(parents=True, exist_ok=True)

        with filepath.open("wb") as f:
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
class SimpleParser:

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


class ParserResource(ConfigurableResource):

    browser:str = 'chrome'
    platform:str = 'windows'
    
    def get_client(self):
        return SimpleParser(self.browser, self.platform)

class DatabaseResource(ConfigurableResource):

    connection_path:str
    
    def get_client(self):
        return DatabaseConnection(self.connection_path)
    

class S3Resource(ConfigurableResource):

    path: str = f'{ROOT.parent.parent}/data'

    def get_client(self):
        return LocalDirConnection(self.path)
