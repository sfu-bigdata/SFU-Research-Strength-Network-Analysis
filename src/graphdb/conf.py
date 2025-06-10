'''
SimpleNamespace Configuration object for Neo4j database
'''
from types import SimpleNamespace
from os import environ

DatabaseConfig = SimpleNamespace(
    httpPorts=('7474', '7474'),
    boltPorts=('7687', '7687'),
    targetAddress = environ.get('TARGET_ADDRESS', '127.0.0.1')
)