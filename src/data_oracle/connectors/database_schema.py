from typing import Type
from ..enums import data_types,Data_Table_Type

class Column:
    def __init__(self, _name:str, _type:Type[data_types], _is_fk:bool=False, _fk_to:str=None):
        self.name = _name
        self.type = _type
        if _is_fk and not isinstance(_fk_to,str):
            raise ValueError(f'_fk_to needs to be a string pointing to a table, but you provided {_fk_to}')

        self.is_fk = _is_fk
        self.fk_table = _fk_to
class Table:

    def __init__(self, _name:str, _columns:list[Column], _type:Type[Data_Table_Type]):
        self.name = _name
        self.columns = _columns
        self.type = _type


