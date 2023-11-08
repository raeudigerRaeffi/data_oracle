from typing import Type
from ..enums import data_types,Data_Table_Type



class BaseDbObject:
    def __str__(self):
        return str(vars(self))

    def __repr__(self):
        return str(vars(self))

class Column(BaseDbObject):
    def __init__(self, _name:str, _type, _is_fk:bool=False, _fk_to:str=None):
        self.name = _name
        self.type = _type
        if _is_fk and not isinstance(_fk_to,str):
            raise ValueError(f'_fk_to needs to be a string pointing to a table, but you provided {_fk_to}')

        self.is_fk = _is_fk
        self.fk_table = _fk_to

    def return_data(self):
        return {
            "name" : self.name,
            "data_type" : self.type
        }

class Table(BaseDbObject):

    def __init__(self, _name:str, _columns:list[Column], _type:Type[Data_Table_Type]):
        self.name = _name
        self.columns = _columns
        self.type = _type

    def return_columns(self):
        return [x.return_data() for x in self.columns]


class Database(BaseDbObject):
    def __init__(self,_name:str):
        self.name = _name
        self.tables = {
            Data_Table_Type.TABLE: {},
            Data_Table_Type.VIEW: {}
        }

    def register_table(self,_table:Type[Table]):
        self.tables[_table.type][_table.name] = _table

    def register_tables(self, _tables:list[Table]):
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






