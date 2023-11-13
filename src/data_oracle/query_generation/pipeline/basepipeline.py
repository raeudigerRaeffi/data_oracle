from ...connectors import BaseDBConnector


class PipelineSqlGen:

    def __init__(self, _connection: BaseDBConnector):
        self.connection = _connection
        self.db = _connection.scan_db()

    def reload_database(self) -> None:

        filter_list = self.db.filter_list
        filter_active = self.db.filter_active

        new_db = self._connection.scan_db()
        new_db.filter_list = filter_list
        new_db.filter_active = filter_active
        new_db.determine_filtered_elements()
        self.db = new_db

    def apply_table_name_filter(self, _table_names: list[str]) -> list[str]:
        self.db.apply_name_filter(_table_names)
        return self.db.get_filtered_tables()

    def apply_table_regex_filter(self, _regex: str) -> list[str]:
        self.db.apply_regex_filter(_regex)
        return self.db.get_filtered_tables()

    def generate_prompt(self):
        pass
