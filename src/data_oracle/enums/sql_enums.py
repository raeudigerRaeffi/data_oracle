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