from .base_db_class import BaseDbObject
class Foreign_Key_Relation(BaseDbObject):
    def __init__(self, _cols: list[str], ref_table: str, ref_cols: list[str]):
        self.constrained_columns = _cols
        self.referred_table = ref_table
        self.referred_columns = ref_cols

    def return_sql_definition(self) -> str:
        _out = f'FOREIGN KEY ({",".join(self.constrained_columns)}) REFERENCES '
        _out += f'{self.referred_table}({",".join(self.referred_columns)})'
        return _out