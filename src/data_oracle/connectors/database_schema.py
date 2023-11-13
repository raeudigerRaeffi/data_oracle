from typing import Type
from ..enums import data_types, Data_Table_Type


class BaseDbObject:
    def __str__(self):
        return str(vars(self))

    def __repr__(self):
        return str(vars(self))


class Foreign_Key_Relation(BaseDbObject):
    def __init__(self, _cols: list[str], ref_table: str, ref_cols: list[str]):
        self.constrained_columns = _cols
        self.referred_table = ref_table
        self.referred_columns = ref_cols

    def return_sql_definition(self) -> str:
        _out = f'FOREIGN KEY ({",".join(self.constrained_columns)}) REFERENCES '
        _out += f'{self.referred_table}({",".join(self.referred_columns)})'
        return _out


class Column(BaseDbObject):
    def __init__(self, _name: str, _type, _is_pk=False, _is_fk: bool = False):
        self.name = _name
        self.type = _type
        self.is_pk = _is_pk
        self.is_fk = _is_fk

    def return_data(self) -> dict:
        return vars(self)

    def return_sql_definition(self) -> str:
        return f'{self.name} {self.type}'


class Table(BaseDbObject):

    def __init__(self,
                 _name: str,
                 _pk_name: str,
                 _columns: list[Column],
                 _type: Type[Data_Table_Type],
                 _fk_relations: list[Foreign_Key_Relation]
                 ):
        self.name = _name
        self.columns = _columns
        self.type = _type
        self.pk = [x for x in _columns if x.is_pk]  # pk can be composed of multiple columns
        self.pk_name = _pk_name
        self.fk_relations = _fk_relations

    def get_cols(self) -> list[Column]:
        return self.columns

    def return_columns_layout(self) -> list[dict]:
        return [x.return_data() for x in self.get_cols()]

    def has_pk(self) -> bool:
        return len(self.pk) > 0

    def code_representation_str(self) -> str:
        _str_repr = ""
        _str_repr += f"CREATE TABLE {self.name}(\n"
        _str_repr += ",\n".join([x.return_sql_definition() for x in self.get_cols()])

        if self.has_pk():
            _str_repr += ",\n"
            if self.pk_name == None:
                _str_repr += f"PRIMARY KEY ({self.pk[0].name})"
            else:
                _str_repr += f"CONSTRAINT {self.pk_name} PRIMARY KEY ({','.join([x.name for x in self.pk])})"
        if len(self.fk_relations) > 0:
            _str_repr += ",\n"
            _str_repr += ",\n".join([x.return_sql_definition() for x in self.fk_relations])
        _str_repr += "\n)"

        return _str_repr


class Database(BaseDbObject):
    def __init__(self, _name: str):
        self.name = _name
        self.tables = []
        self.filter_active = False
        self.filter_list = []

    def register_table(self, _table: Table) -> None:
        self.tables.append(_table)

    def register_tables(self, _tables: list[Table]) -> None:
        for _table in _tables:
            self.register_table(_table)

    def get_tables(self) -> list[Type[Table]]:
        if self.filter_active:
            pass
        return self.tables

    def return_code_repr_schema(self, exclude_views=False) -> str:
        _all_tables = self.get_tables()
        if exclude_views:
            view_filter = lambda x: True if x.type != Data_Table_Type.VIEW else False
            _all_tables = filter(view_filter, _all_tables)
        return "\n\n".join([x.code_representation_str() for x in _all_tables])

    def return_text_repr_schema(self, exclude_views=False) -> str:
        _out = []
        _all_tables = self.get_tables()
        if exclude_views:
            view_filter = lambda x: True if x.type != Data_Table_Type.VIEW else False
            _all_tables = filter(view_filter, _all_tables)

        for table in _all_tables:
            table_text_repr = f'{table.name} : '
            table_text_repr += " ,".join([x.name for x in table.get_cols()])
            _out.append(table_text_repr)

        return "\n".join(_out)
