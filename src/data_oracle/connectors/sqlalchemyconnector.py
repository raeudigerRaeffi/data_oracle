import asyncio
from .baseconnector import BaseDBConnector
from typing import Type
from sqlalchemy import create_engine, URL, inspect, text

from overrides import override

class connectionDetails:

    def __init__(self,database_type,username,password,host,port,database_name):
        self.database_type = database_type
        self.username = username
        self.password = password
        self.host = host
        self.port = port
        self.database_name = database_name


class SqlAlchemyConnector(BaseDBConnector):
    """
    Connector available for all
    """
    def __init__(self,connection_data:Type[connectionDetails]):
        super().__init__(connection_data)

    @override
    def connect(self,connection_data:Type[connectionDetails]):
        #connection_data.database_type
        drivername_mapper = {
            'MySQL' : 'mysql+pymysql',
            'PostgreSQL' : 'postgresql+psycopg2',
            'Oracle' : 'oracle+oracledb',
            'Microsoft SQL Server' : 'mssql+pyodbc',
            'SQLite' : 'sqlite',
        }
        driver = drivername_mapper[connection_data.database_type]
        url_object = URL.create(
            driver,
            username = connection_data.username,
            password = connection_data.password,
            host = connection_data.host,
            port = connection_data.port,
            database= connection_data.database_name,
        )
        return create_engine(url_object)

    @override
    def is_available(self):
        """
        Returns Boolean whether connections is successful
        """
        try:
            self.connection.connect()
            return True
        except:
            return False

    @override
    def return_table_names(self):
        """
        Returns list of table names
        """
        return inspect(self.connection).get_table_names()

    @override
    def return_view_names(self):
        """
        Returns list of view names
        """
        return inspect(self.connection).get_view_names()

    def return_all_table_column_info(self,table_name:str):
        return inspect(self.connection).get_columns(table_name)


    @override
    def return_table_columns(self,table_name:str):
        """
        Returns list of columns of table
        """
        all_info = self.return_all_table_column_info(table_name)
        return [x['name'] for x in all_info]

    @override
    def execute_sql_statement(self,_sql:str):
        """
        @_sql:str
        Returns result of sql statement
        """
        with self.connection.connect() as conn:
            result = conn.execute(text(_sql))
        return result