from data_oracle.db_schema import Column,Table,Database
def test_db_filter():
    column_list = [
        Column("Col1", "INTEGER"),
        Column("Col2", "INTEGER"),
        Column("Col3", "INTEGER"),
        Column("Col4", "INTEGER")
    ]
    table_list = [
        Table("T1", None, column_list, "Table", []),
        Table("T2", None, column_list, "Table", []),
        Table("T3", None, column_list, "View", []),
        Table("Test2", None, column_list, "View", []),
    ]
