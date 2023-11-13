from ...connectors import BaseDBConnector


class PipelineSqlGen:

    def __init__(self, _connection:BaseDBConnector):
        self.connection = _connection
        pass