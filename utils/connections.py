import duckdb
import pandas as pd
from selenium import webdriver
from selenium.webdriver.firefox.options import Options
from dataclasses import dataclass,field
from dagster import resource
from contextlib import contextmanager

GEKO_DRIVER_PATH = 'utils/geckodriver'



@dataclass
class DatabaseConnection:
    connection_path: str 
    
    def __post_init__(self):
        self.connection = duckdb.connect(self.connection_path)

    def query(self,SQL):
        return self.connection.execute(SQL)
    
    def close(self):
        self.connection.close()

    def append_df(self,df:pd.DataFrame,table_name:str):
        self.connection.execute(f'''
                    CREATE TABLE IF NOT EXISTS {table_name} as select * from df TABLESAMPLE 0;
                    INSERT INTO {table_name}({",".join(df.columns)}) select * from df;
                    ''')

@dataclass
class SeleniumConnection:

    driver_path:str = GEKO_DRIVER_PATH
    headers:dict = field(default_factory=dict)

    def __post_init__(self):
        firefox_options = Options()
        firefox_options.headless = True
        firefox_options.add_argument(f"HEADERS={self.headers}")
        self.parser = webdriver.Firefox(options=firefox_options,
                                        executable_path=self.driver_path)

    def get(self,url):
        self.parser.get(url)
        self.html_data =  self.parser.find_element('xpath',"//*").get_attribute("outerHTML")
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


@resource(config_schema={"headers": dict,'driver_path':str})
def parser_resource(init_context):
    try:
        driver = SeleniumConnection(**init_context.resource_config)
        yield driver
    finally :
        driver.close()

