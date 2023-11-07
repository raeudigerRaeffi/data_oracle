from enum import Enum


class Sql_Alchemy_Types(Enum):
    MYSQL = 'MySQL'
    POSTGRESQL = 'PostgreSQL'
    ORACLE = 'Oracle'
    MICROSOFTSQLSERVER = 'Microsoft SQL Server'
    SQLITE = 'SQLite'

class Sql_Types(Enum):
    MYSQL = 'MySQL'
    POSTGRESQL = 'PostgreSQL'
    ORACLE = 'Oracle'
    MICROSOFTSQLSERVER = 'Microsoft SQL Server'
    SQLITE = 'SQLite'
    BIGQUERY = 'BigQuery'

class data_types(Enum):
    INTEGER = "INT"
    VARCHAR = "VARCHAR"
    DATE = "DATE"

class Data_Table_Type(Enum):
    TABLE = "Table"
    VIEW = "View"