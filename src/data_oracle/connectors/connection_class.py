from typing import Union


class connection_details:

    def __init__(self, database_type: str, username: str, password: str, host: str, port: int, database_name: str,
                 ssl =False, ssl_credentials: str = None):
        self.database_type = database_type
        self.username = username
        self.password = password
        self.host = host
        self.port = port
        self.database_name = database_name
        self.ssl = ssl
        self.ssl_credentials = ssl_credentials


class file_connection:
    def __init__(self,_path,_name,_type="SQLite"):
        self.path = _path
        self.database_name = _name
        self.type = _type

class csv_connection:
    def __init__(self, file_path: str):
        self.database_name = file_path.split(".")[0]
        self.file_path = file_path


connection_info = Union[connection_details, csv_connection,file_connection]
