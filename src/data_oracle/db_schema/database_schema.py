from typing import Type
from src.data_oracle.enums import data_types, Data_Table_Type
from .base_db_class import BaseDbObject
from .foreign_key_schema import Foreign_Key_Relation
from .filter import filter
from ..enums import Filter_Type
import re


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
                 _type: Data_Table_Type,
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
        self.name: str = _name
        self.tables: list[Table] = []
        self.filter_active: bool = False
        self.filter_list: list[filter] = []
        self.filtered_tables: list[Table] = []

    def register_table(self, _table: Table) -> None:
        self.tables.append(_table)

    def register_tables(self, _tables: list[Table]) -> None:
        for _table in _tables:
            self.register_table(_table)

    def release_filters(self) -> None:
        self.filter_active = False
        self.filter_list = []
        self.filtered_tables = []

    def determine_filtered_elements(self) -> None:
        filter_name_hashmap = {x: None for a in self.filter_list if a.classification == Filter_Type.NAME for x in
                               a.value}
        regex_filters = [re.compile(x.value) for x in self.filter_list if x.classification == Filter_Type.REGEX]

        for _table in self.tables:
            matched_regex = False
            for reg_patter in regex_filters:
                if reg_patter.match(_table.name):
                    matched_regex = True
            if _table.name not in filter_name_hashmap and not matched_regex:
                self.filtered_tables.append(_table)

    def apply_filter(self, table_names: list[str] = None, regex_filter: str = None) -> None:
        self.filtered_tables = []
        if table_names == None and regex_filter == None:
            raise ValueError(f"The function needs to be called with a valid argument")
        if table_names != None:
            new_filter = filter(table_names, Filter_Type.NAME)
        else:
            new_filter = filter(regex_filter, Filter_Type.REGEX)
        self.filter_active = True
        self.filter_list.append(new_filter)
        self.determine_filtered_elements()

    def apply_name_filter(self, _table_names: list[str]) -> None:
        self.apply_filter(table_names=_table_names)

    def apply_regex_filter(self, _regex_filter: str) -> None:
        self.apply_filter(regex_filter=_regex_filter)

    def get_filtered_tables(self) -> list[str]:
        return [x.name for x in list(set(self.tables) - set(self.filtered_tables))]

    def get_tables(self) -> list[Table]:
        if self.filter_active:
            return self.filtered_tables
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
