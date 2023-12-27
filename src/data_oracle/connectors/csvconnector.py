from .baseconnector import BaseDBConnector
from typing import Type
from ..db_schema import Column, Table, Foreign_Key_Relation
from .connection_class import connection_info, csv_connection
from overrides import override
import pandas as pd


class CsvConnector(BaseDBConnector):
    """
    Connector available for all Sql Dbs that can be accessed with Sqlalchemy
    """

    def __init__(self, _filepath: str):
        super().__init__(csv_connection(_filepath))
        # self.inspection = inspect(self.connection)
        # self.fk_relations = {}
        # self.pk = {}

    @override
    def connect(self, connec: connection_info):
        return pd.read_csv(connec.file_path)

    @override
    def is_available(self):
        return True

    @override
    def return_table_names(self):
        return [self.db.name]

    @override
    def return_view_names(self):
        """
        Returns list of view names
        """
        return []
