'''
SimpleNamespace Configuration object for Neo4j database
'''
from types import SimpleNamespace
from os import environ
from dataclasses import dataclass
from enum import Enum
from ..processing.conf import NodeType

DatabaseConfig = SimpleNamespace(
    databaseName = 'neo4j',
    httpClient = '7474',
    httpDB = '7474',
    boltClient = '7687',
    boltDB = '7687',
    targetAddress = environ.get('TARGET_ADDRESS', '127.0.0.1')
)

class GraphType(Enum):
    NODE = 0
    RELATIONSHIP = 1

@dataclass
class GraphObject:
    prefix: str
    name: str

ObjectNames = {
    NodeType.author : GraphObject(prefix='A', name='author'),
    NodeType.funder : GraphObject(prefix='F', name='funder'),
    NodeType.institution : GraphObject(prefix='I', name='institution'),
    NodeType.source : GraphObject(prefix='S', name='source'),
    NodeType.work : GraphObject(prefix='W', name='work'),
    NodeType.topic : GraphObject(prefix='T', name='topic'),
    NodeType.subfield : GraphObject(prefix='SFLD', name='subfield'),
    NodeType.field : GraphObject(prefix='FLD', name='field'),
    NodeType.domain : GraphObject(prefix='D', name='domain')
}