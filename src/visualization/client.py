from graphdatascience import GraphDataScience
from os import environ

class Client(object):
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(Client, cls).__new__(cls)
        return cls._instance
    
    def __init__(self):

        if not hasattr(self, '_initialized'):
            URI = environ.get('CONNECTION_URI','bolt://localhost:7687')
            USER = environ.get('CONNECTION_USER', '')
            PW = environ.get('CONNECTION_PASSWORD', '')

            self._gds_client = None
            try:
                self._gds_client = GraphDataScience(
                    URI, auth=(USER, PW)
                )
                print("Connected to Neo4j database.")
            except Exception as e:
                print(f'Error connecting to database: {e}')
                self._gds_client = None
                raise e
            self._initialized = True
    
    def __call__(self, query: str):
        try:
            return self._gds_client.run_cypher(query)
        except Exception as e:
            print(f'Error executing query: {e}')
            raise e