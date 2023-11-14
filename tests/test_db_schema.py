from data_oracle.db_schema import Column, Table, Database


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
    scanned_db.apply_table_name_filter(["Persons", "T1"])
    scanned_db.apply_table_name_filter(["Test3", "Test4"])
    scanned_db.apply_table_regex_filter("Test[0-9]+")

    assert set(["T1", "Test2"]) == set(scanned_db.get_filtered_tables())


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
    scanned_db.apply_table_name_filter(["Persons", "T1"])
    scanned_db.apply_table_name_filter(["Test3", "Test4"])
    scanned_db.apply_table_regex_filter("Test[0-9]+")
    scanned_db.release_filters()

    assert set([]) == set(scanned_db.filtered_content)


def test_db_get_tables():
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
    scanned_db.apply_table_name_filter(["Persons", "T1"])
    scanned_db.apply_table_name_filter(["Test3", "Test4"])
    scanned_db.apply_table_regex_filter("Test[0-9]+")
    filtered_tables = [x.name for x in scanned_db.get_tables()]

    non_filter_db = Database("test")
    non_filter_db.register_tables(table_list)
    non_filtered_tables = [x.name for x in non_filter_db.get_tables()]
    assert (set(["T1", "T2", "T3", "Test2"]) == set(non_filtered_tables)
            and
            set(["T2", "T3"]) == set(filtered_tables)
            )


def test_filter_columns():
    column_list = [
        Column("Col1", "INTEGER", True, True),
        Column("Col2", "INTEGER"),
        Column("Col3", "INTEGER"),
        Column("Test1", "INTEGER"),
        Column("Col4", "INTEGER")
    ]
    table_list = [
        Table("T1", None, column_list, "Table", []),
        Table("T2", None, column_list + [Column("Test2", "INTEGER", True, True)], "Table", [])
    ]
    scanned_db = Database("test")
    scanned_db.register_tables(table_list)

    name_filter = {
        "T1": ["Col1", "Col2", "Col3"]
    }

    regex_filter = {
        "T2": "Test[0-9]+"
    }
    scanned_db.apply_column_name_filter(name_filter)
    scanned_db.apply_column_regex_filter(regex_filter)
    assert (set(["Col1", "Test1", "Col4"]) == set([x.name for x in scanned_db.get_tables()[0].get_cols()])
            and
            set(["Col1", "Col2", "Col3", "Test2", "Col4"])
            == set([x.name for x in scanned_db.get_tables()[1].get_cols()])
            )
