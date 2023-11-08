from enum import Enum


class BaseEnum(str,Enum):
    def __str__(self):
        return str(self.value)

    def __repr__(self):
        return str(self.value)


class Sql_Alchemy_Types(BaseEnum):
    MYSQL = 'MySQL'
    POSTGRESQL = 'PostgreSQL'
    ORACLE = 'Oracle'
    MICROSOFTSQLSERVER = 'Microsoft SQL Server'
    SQLITE = 'SQLite'

class Sql_Types(BaseEnum):
    MYSQL = 'MySQL'
    POSTGRESQL = 'PostgreSQL'
    ORACLE = 'Oracle'
    MICROSOFTSQLSERVER = 'Microsoft SQL Server'
    SQLITE = 'SQLite'
    BIGQUERY = 'BigQuery'

class data_types(BaseEnum):
    INTEGER = "INT"
    VARCHAR = "VARCHAR"
    DATE = "DATE"

class Data_Table_Type(BaseEnum):
    TABLE = "Table"
    VIEW = "View"


