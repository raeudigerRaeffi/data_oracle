from data_oracle.db_schema import Column,Table,Database
def test_db_apply_filter():
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
    scanned_db = Database("test")
    scanned_db.register_tables(table_list)
    scanned_db.apply_name_filter(["Persons", "T1"])
    scanned_db.apply_name_filter(["Test3", "Test4"])
    scanned_db.apply_regex_filter("Test[0-9]+")

    assert set(["T1","Test2"]) == set(scanned_db.get_filtered_tables())

def test_db_release_filter():
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
    scanned_db = Database("test")
    scanned_db.register_tables(table_list)
    scanned_db.apply_name_filter(["Persons", "T1"])
    scanned_db.apply_name_filter(["Test3", "Test4"])
    scanned_db.apply_regex_filter("Test[0-9]+")
    scanned_db.release_filters()

    assert set([]) == set(scanned_db.filtered_tables)

