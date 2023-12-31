from .baseconnector import BaseDBConnector
from typing import Type
from sqlalchemy import create_engine, URL, inspect, text
from ..db_schema import Column, Table, Foreign_Key_Relation
from .connection_class import connection_info, connection_details, file_connection
from overrides import override


class SqlAlchemyConnector(BaseDBConnector):
    """
    Connector available for all Sql Dbs that can be accessed with Sqlalchemy
    """

    def __init__(self, connection_data: connection_info):
        super().__init__(connection_data)
        self.inspection = inspect(self.connection)
        self.fk_relations = {}
        self.pk = {}

    @override
    def connect(self, connection_data):
        drivername_mapper = {
            'MySQL': 'mysql+pymysql',
            'PostgreSQL': 'postgresql+psycopg2',
            'Oracle': 'oracle+oracledb',
            'Microsoft SQL Server': 'mssql+pyodbc',
            'SQLite': 'sqlite',
        }
        if type(connection_data) == connection_details:

            driver = drivername_mapper[connection_data.database_type]
            self.type = connection_data.database_type
            url_object = URL.create(
                driver,
                username=connection_data.username,
                password=connection_data.password,
                host=connection_data.host,
                port=connection_data.port,
                database=connection_data.database_name,
            )

            if connection_data.ssl:
                ssl_args = {"ssl": {
                    # or change to verify-ca or verify-full based on your requirement
                    'ca': connection_data.ssl_credentials,  # path to your .crt file
                }}
                return create_engine(url_object, connect_args=ssl_args)

            return create_engine(url_object)
        elif type(connection_data) == file_connection:
            return create_engine("sqlite:///" + connection_data.path)

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
        Returns list of table names and detects fk and pk columns
        """
        table_names = self.inspection.get_table_names()
        for _table in table_names:
            self.pk[_table] = {x: x for x in self.inspection.get_pk_constraint(_table)["constrained_columns"]}
            self.fk_relations[_table] = {}
            fk_relations = self.inspection.get_foreign_keys(_table)
            for fk_relation in fk_relations:
                for _col in fk_relation["constrained_columns"]:
                    self.fk_relations[_table][_col] = fk_relation["referred_table"]

        return table_names

    @override
    def return_view_names(self):
        """
        Returns list of view names
        """
        view_names = self.inspection.get_view_names()
        for _table in view_names:
            self.pk[_table] = {x: x for x in self.inspection.get_pk_constraint(_table)["constrained_columns"]}
            self.fk_relations[_table] = {}
            fk_relations = self.inspection.get_foreign_keys(_table)
            for fk_relation in fk_relations:
                for _col in fk_relation["constrained_columns"]:
                    self.fk_relations[_table][_col] = fk_relation["referred_table"]

        return view_names

    def return_all_table_column_info(self, table_name: str) -> list[Column]:
        out = []
        all_cols = self.inspection.get_columns(table_name)
        for _col in all_cols:
            _name = _col["name"]
            _is_pk = False
            _is_fk = False
            _fk_to = None
            if _name in self.fk_relations[table_name]:
                _is_fk = True
            if _name in self.pk[table_name]:
                _is_pk = True

            new_col = Column(_name, _col["type"], _is_pk, _is_fk)
            out.append(new_col)

        return out

    @override
    def return_table_columns(self, table_name, _table_type) -> Table:
        """
        Returns list of columns of table
        """
        all_info = self.return_all_table_column_info(table_name)
        _pk_name = self.inspection.get_pk_constraint(table_name)['name']
        fk_relations = [Foreign_Key_Relation(
            x["constrained_columns"],
            x["referred_table"],
            x["referred_columns"])
            for x in self.inspection.get_foreign_keys(table_name)]
        return Table(table_name, _pk_name, all_info, _table_type, fk_relations)

    @override
    def execute_sql_statement(self, _sql, _max_rows = None):
        """
        @_sql:str
        Returns result of sql statement
        """
        results = []
        with self.connection.connect() as conn:
            sql_res_conn = conn.execute(text(_sql))
            results.append(sql_res_conn.keys())
            counter = 0
            for _row in sql_res_conn:
                results.append(_row)
                counter += 1
                if _max_rows is not None and _max_rows < counter:
                    break

        return results

