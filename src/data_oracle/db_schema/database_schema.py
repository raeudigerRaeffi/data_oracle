from typing import Dict, Union
from .base_db_class import BaseDbObject
from .foreign_key_schema import Foreign_Key_Relation
from .filterobject import FilterObject, EmbeddingContainer
from ..enums import Filter_Type, Data_Table_Type
from .utils import calc_embedding
import re



class FilterClass:

    def __init__(self):
        self.filter_active: bool = False
        self.filter_list: list[FilterObject] = []
        self.filtered_content: list[Table | Column] = []
        self.embedding_filter: EmbeddingContainer | None = None

    def release_filters(self) -> None:
        """
        Removes all active filters
        @return: None
        """
        self.filter_active = False
        self.filter_list = []
        self.filtered_content = []
        self.embedding_filter = None

    def determine_filtered_elements(self, content) -> None:
        """
        Determines which elements are not filtered
        @param content: list of tables or columns
        @return: None
        """
        filter_name_hashmap = {}
        regex_filters = []
        is_column = isinstance(content[0], Column)

        for _filter in self.filter_list:
            if _filter.classification == Filter_Type.NAME:
                for filter_name in _filter.value:
                    filter_name_hashmap[filter_name] = None
            elif _filter.classification == Filter_Type.REGEX:
                regex_filters.append(re.compile(_filter.value))

        for _item in content:
            matched_regex = False
            if is_column:
                if _item.is_pk or _item.is_fk:
                    self.filtered_content.append(_item)
                    continue
            for reg_patter in regex_filters:
                if reg_patter.match(_item.name):
                    matched_regex = True
            if _item.name not in filter_name_hashmap and not matched_regex:
                if self.embedding_filter is not None:
                    if self.embedding_filter.embedding @ _item.embedding.T <= self.embedding_filter.threshold:
                        self.filtered_content.append(_item)
                else:
                    self.filtered_content.append(_item)

    def apply_filter(self,
                     target,
                     content_names: list[str] = None,
                     regex_filter: str = None,
                     embedding_filter: FilterObject = None) -> None:
        """
        Function which applies an active function to the object
        @param embedding_filter: Fdy
        @param target: list of columns or tables
        @param content_names: list of object names
        @param regex_filter: regex pattern
        @return: None
        """
        self.filtered_content = []
        if content_names is None and regex_filter is None and embedding_filter is None:
            raise ValueError(f"The function needs to be called with a valid argument")
        if content_names is not None:
            new_filter = FilterObject(value=content_names, _type=Filter_Type.NAME)
            self.filter_list.append(new_filter)
        elif regex_filter is not None:
            new_filter = FilterObject(value=regex_filter, _type=Filter_Type.REGEX)
            self.filter_list.append(new_filter)
        else:
            self.embedding_filter = embedding_filter.value
        self.filter_active = True
        self.determine_filtered_elements(target)


class Column(BaseDbObject):
    def __init__(self, _name: str, _type, _is_pk=False, _is_fk: bool = False,_enums:list[str]=None):
        self.name = _name
        self.type = _type
        self.is_pk = _is_pk
        self.is_fk = _is_fk
        self.enums = _enums
        self.embedding = None

    def return_data(self) -> dict:
        """
        Returns dict represenation of object
        @return: Dict containing all self variables
        """
        return vars(self)

    def return_sql_definition(self) -> str:
        """
        Returns column sql def
        @return: str sql representation
        """
        if self.enums is not None:
            return f"""{self.name} ENUM({",".join([ '"'+x+'"' for x in self.enums])})"""
        return f'{self.name} {self.type}'


class Table(BaseDbObject, FilterClass):

    def __init__(self,
                 _name: str,
                 _pk_name: str,
                 _columns: list[Column],
                 _type: Data_Table_Type,
                 _fk_relations: list[Foreign_Key_Relation]
                 ):
        super().__init__()
        self.name = _name
        self.columns = _columns
        self.type = _type
        self.pk = [x for x in _columns if x.is_pk]  # pk can be composed of multiple columns
        self.pk_name = _pk_name
        self.fk_relations = _fk_relations
        self.embedding = None

    def get_cols(self) -> list[Column]:
        """
        Returns all columns based on active filters
        @return: All columns
        """
        if self.filter_active:
            return self.filtered_content
        return self.columns

    def return_columns_layout(self) -> list[dict]:
        return [x.return_data() for x in self.get_cols()]

    def has_pk(self) -> bool:
        """
        Returns whether the table has a primary key
        @return: Boolean indicating if primary key is present
        """
        return len(self.pk) > 0

    def apply_column_name_filter(self, _column_names: list[str]) -> None:
        """
        Applies filter which filters columns based on exact name matching
        @param _column_names: list of column names which are to be filtered
        @return: None
        """
        self.apply_filter(self.columns, content_names=_column_names)

    def apply_column_regex_filter(self, _regex_filter: str) -> None:
        """
        Applies filter which filters columns based on whether their name is matched by a regex pattern
        @param _regex_filter: string representation of regex pattern
        @return: None
        """
        self.apply_filter(self.columns, regex_filter=_regex_filter)

    def get_filtered_columns(self) -> list[str]:
        """
        Returns list of column names that are filtered out
        @return: list of column names
        """
        if self.filter_active:
            return [x.name for x in list(set(self.columns) - set(self.filtered_content))]
        else:
            return []

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

    def apply_embedding_filter(self, embed_filter: FilterObject):
        self.apply_filter(self.columns, embedding_filter=embed_filter)


class Database(BaseDbObject, FilterClass):
    def __init__(self, _name: str):
        super().__init__()
        self.name: str = _name
        self.tables: list[Table] = []

    def register_table(self, _table: Table) -> None:
        """
        Appends table to database obj
        @param _table: Table object
        @return: None
        """
        self.tables.append(_table)

    def register_tables(self, _tables: list[Table]) -> None:
        """
        Appends list of Tables to database obj
        @param _tables: list of Table objects
        @return: None
        """
        for _table in _tables:
            self.register_table(_table)

    def apply_table_name_filter(self, _table_names: list[str]) -> None:
        """
        Applies a filter to remove all tables by exact name matching
        @param _table_names: list of table names
        @return: None
        """
        self.apply_filter(self.tables, content_names=_table_names)

    def apply_table_regex_filter(self, _regex_filter: str) -> None:
        """
        Applies a filter to remove all tables by regex pattern matching based on table name
        @param _regex_filter: regex string pattern
        @return: None
        """
        self.apply_filter(self.tables, regex_filter=_regex_filter)

    def apply_column_name_filter(self, filter_list: Dict[str, list[str]]) -> None:
        """
        Applies a exact name filter to columns in a given table. Table is specified in key and the applied filters in the value list
        @param filter_list: dict object with table name as key and list of column names as value
        @return: None
        """
        for table in self.tables:
            if table.name in filter_list:
                table.apply_column_name_filter(filter_list[table.name])

    def apply_column_regex_filter(self, filter_list: Dict[str, str]) -> None:
        """
        Applies a regex name filter to columns in a given table. Table is specified in key and the value is the regex pattern
        @param filter_list: dict object with table name as key and regex pattern as value
        @return:
        """
        for table in self.tables:
            if table.name in filter_list:
                table.apply_column_regex_filter(filter_list[table.name])

    def apply_embedding_filter(self,
                               nl_question: str,
                               embedding,
                               threshold: float,
                               applicable_table: bool) -> None:
        """

        @param nl_question:
        @param embedding:
        @param threshold:
        @param applicable_table:
        @return:
        """
        embed_filter = FilterObject(value=embedding,
                                    _type=Filter_Type.EMBEDDING,
                                    nl_question=nl_question,
                                    threshold=threshold
                                    )
        if applicable_table:
            self.apply_filter(self.tables, embedding_filter=embed_filter)
        for _table in self.tables:
            _table.apply_embedding_filter(embed_filter)

    def release_column_filters(self) -> None:
        """
        Releases all filters in the db
        @return:
        """
        for table in self.tables:
            table.release_filters()

    def get_filtered_tables(self) -> list[str]:
        """
        Determine which tables are removed by filters
        @return: list of table names
        """
        if self.filter_active:
            return [x.name for x in list(set(self.tables) - set(self.filtered_content))]
        else:
            return []

    def get_tables(self) -> list[Table]:
        """
        Get all tables based on active filters
        @return: list of Table objects
        """
        if self.filter_active:
            return self.filtered_content
        return self.tables

    def apply_embedding_model(self, embedding_model, embedding_tokenizer) -> None:
        """
        Applies embedding model to all table names and column names and fills the respective embedding values
        @param embedding_model: Huggingface language model
        @param embedding_tokenizer: Huggingface tokenizer model
        @return: None
        """
        for table in self.tables:
            table.embedding = calc_embedding(embedding_model, embedding_tokenizer, table.name)
            for column in table.columns:
                column.embedding = calc_embedding(embedding_model, embedding_tokenizer, column.name)

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
