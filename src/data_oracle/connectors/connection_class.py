
class connection_details:

    def __init__(self,database_type:str,username:str,password:str,host:str,port:int,database_name:str):
        self.database_type = database_type
        self.username = username
        self.password = password
        self.host = host
        self.port = port
        self.database_name = database_name
