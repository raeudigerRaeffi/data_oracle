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
                 _fk_relations: list[Foreign_Key_Relation]):
        self.name = _name
        self.columns = _columns
        self.type = _type
        self.pk = [x for x in _columns if x.is_pk]  # pk can be composed of multiple columns
        self.pk_name = _pk_name
        self.fk_relations = _fk_relations

    def return_columns(self) -> list[dict]:
        return [x.return_data() for x in self.columns]

    def has_pk(self) -> bool:
        return len(self.pk) > 0

    def code_representation_str(self) -> str:
        _str_repr = ""
        _str_repr += f"CREATE TABLE {self.name}(\n"
        _str_repr += ",\n".join([x.return_sql_definition() for x in self.columns])

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
        self.tables = {
            Data_Table_Type.TABLE: {},
            Data_Table_Type.VIEW: {}
        }

    def register_table(self, _table: Type[Table]) -> None:
        self.tables[_table.type][_table.name] = _table

    def register_tables(self, _tables: list[Table]) -> None:
        for _table in _tables:
            self.register_table(_table)

    def return_layout(self):
        out = {}
        for table_type in self.tables:
            out[table_type] = {}
            current_table_type = self.tables[table_type]
            for _table in current_table_type:
                out[table_type][_table] = current_table_type[_table].return_columns()
        return out

    def return_tables(self, _key: Data_Table_Type) -> list[Type[Table]]:
        return [self.tables[_key][x] for x in self.tables[_key].keys()]

    def return_code_repr_schema(self, include_views=False) -> str:
        _all_tables = self.return_tables(Data_Table_Type.TABLE)
        if include_views:
            _all_tables += self.return_tables(Data_Table_Type.VIEW)
        return "\n\n".join([x.code_representation_str() for x in _all_tables])

    def return_text_repr_schema(self, include_views=False) -> str:
        _out = []
        _all_tables = self.return_tables(Data_Table_Type.TABLE)
        if include_views:
            _all_tables += self.return_tables(Data_Table_Type.VIEW)

        for table in _all_tables:
            table_text_repr = f'{table.name} : '
            table_text_repr += " ,".join([x.name for x in table.columns])
            _out.append(table_text_repr)

        return "\n".join(_out)
