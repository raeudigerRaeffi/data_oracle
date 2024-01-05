from typing import Union
from .connection_class import connection_info
from ..db_schema import Database, Table
from ..enums import Data_Table_Type




class BaseDBConnector:

    def __init__(self, connection_data: connection_info):
        self.connection_data = connection_data
        self.type = None
        self.connection = self.connect(connection_data)
        self.db = self.register_db(connection_data)

    def connect(self, connection_data: connection_info):
        """
        Returns a connection object to DB
        """
        pass

    def register_db(self, connection_data: connection_info):
        return Database(connection_data.database_name)

    def is_available(self):
        """
        Returns Boolean whether connections is successful
        """
        pass

    def return_table_names(self):
        """
        Returns list of table names
        """
        pass

    def return_view_names(self):
        """
        Returns list of view names
        """
        pass

    def return_table_columns(self, table_name: str, _table_type,scan_enums:bool) -> Table:
        """
        @table_name:str
        Returns list of columns of table
        """
        pass

    def execute_sql_statement(self, _sql,_max_rows):
        """
        @_sql:str
        Returns result of sql statement
        """

        pass

    def return_table_column_info(self,scan_enums) -> list[Table]:
        """
        Returns dictionary containing table_name : [column_name] pairs
        """
        all_tables = self.return_table_names()
        return [self.return_table_columns(x, Data_Table_Type.TABLE,scan_enums) for x in all_tables]

    def return_view_column_info(self,scan_enums) -> list[Table]:
        """
        Returns list containing table_name : [column_name] pairs
        """
        all_tables = self.return_view_names()
        return [self.return_table_columns(x, Data_Table_Type.VIEW,scan_enums) for x in all_tables]

    def return_db_layout(self) -> Database:
        return self.db

    def scan_db(self,scan_enums=False) -> Database:
        self.db.register_tables(self.return_view_column_info(scan_enums))
        self.db.register_tables(self.return_table_column_info(scan_enums))
        return self.return_db_layout()
