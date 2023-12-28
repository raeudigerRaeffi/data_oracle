from .baseconnector import BaseDBConnector
from typing import Type
from ..db_schema import Column, Table, Foreign_Key_Relation
from .connection_class import connection_info, csv_connection
from overrides import override
import pandas as pd


class ExcelConnector(BaseDBConnector):

    def __init__(self, _filepath: str):
        super().__init__(csv_connection(_filepath))
        # self.inspection = inspect(self.connection)
        # self.fk_relations = {}
        # self.pk = {}

    @override
    def connect(self, connection_data: connection_info):
        return pd.ExcelFile(connection_data.file_path)

    @override
    def is_available(self):
        return True

    @override
    def return_table_names(self) -> list[str]:
        return self.connection.sheet_names

    @override
    def return_view_names(self):
        """
        Returns list of view names
        """
        return []

    def return_all_table_column_info(self, table_name: str) -> list[Column]:
        out = []
        df = pd.read_excel(self.connection_data.file_path,table_name)
        for column in df.columns:
            data_type = str(df[column].dtypes)
            col_type = None
            if "int" in data_type:
                col_type = "INT"
            elif "float" in data_type:
                col_type = "FLOAT"
            elif "datetime" in data_type:
                col_type = "DATE"
            elif df[column].dtypes == "object":
                col_type = "TEXT"

            new_col = Column(column, col_type, False, False)
            out.append(new_col)

        return out

    @override
    def return_table_columns(self, table_name, _table_type) -> Table:
        all_col = self.return_all_table_column_info(table_name)
        return Table(table_name, None, all_col, _table_type, [])

    @override
    def execute_sql_statement(self, _sql):
        """
        @_sql:str
        Returns result of sql statement
        """
        return ""
