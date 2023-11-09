from typing import Type
from ..enums import data_types, Data_Table_Type


class BaseDbObject:
    def __str__(self):
        return str(vars(self))

    def __repr__(self):
        return str(vars(self))


class Column(BaseDbObject):
    def __init__(self, _name: str, _type, _is_pk=False, _is_fk: bool = False, _fk_to: str = None):
        self.name = _name
        self.type = _type
        self.is_pk = _is_pk
        if _is_fk and not isinstance(_fk_to, str):
            raise ValueError(f'_fk_to needs to be a string pointing to a table, but you provided {_fk_to}')

        self.is_fk = _is_fk
        self.fk_table = _fk_to

    def return_data(self):
        return vars(self)


class Table(BaseDbObject):

    def __init__(self, _name: str, _pk_name: str, _columns: list[Column], _type: Type[Data_Table_Type]):
        self.name = _name
        self.columns = _columns
        self.type = _type
        self.pk = [x for x in _columns if x.is_pk]  # pk can be composed of multiple columns
        self.pk_name = _pk_name

    def return_columns(self):
        return [x.return_data() for x in self.columns]

    def has_pk(self):
        return len(self.pk) > 0


class Database(BaseDbObject):
    def __init__(self, _name: str):
        self.name = _name
        self.tables = {
            Data_Table_Type.TABLE: {},
            Data_Table_Type.VIEW: {}
        }

    def register_table(self, _table: Type[Table]):
        self.tables[_table.type][_table.name] = _table

    def register_tables(self, _tables: list[Table]):
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
