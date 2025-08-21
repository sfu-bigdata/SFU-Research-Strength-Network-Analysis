'''
SimpleNamespace Configuration object for Neo4j database
'''
from types import SimpleNamespace
from os import environ
from dataclasses import dataclass
from enum import Enum
from config import NodeType

DatabaseConfig = SimpleNamespace(
    databaseName = 'neo4j',
    httpClient = environ.get('HTTP_PORT', '7474'),
    httpDB = environ.get('HTTP_PORT', '7474'),
    boltClient = environ.get('BOLT_PORT', '7687'),
    boltDB = environ.get('BOLT_PORT', '7687'),
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
    NodeType.author : GraphObject(prefix='A', name=NodeType.author.value),
    NodeType.funder : GraphObject(prefix='F', name=NodeType.funder.value),
    NodeType.SFU_U15_institution : GraphObject(prefix='I', name=NodeType.SFU_U15_institution.value),
    NodeType.source : GraphObject(prefix='S', name=NodeType.source.value),
    NodeType.work : GraphObject(prefix='W', name=NodeType.work.value),
    NodeType.topic : GraphObject(prefix='T', name=NodeType.topic.value),
    NodeType.subfield : GraphObject(prefix='SFLD', name=NodeType.subfield.value),
    NodeType.field : GraphObject(prefix='FLD', name=NodeType.field.value),
    NodeType.domain : GraphObject(prefix='D', name=NodeType.domain.value),
    NodeType.geographic : GraphObject(prefix='GEO', name=NodeType.geographic.value),
    NodeType.affiliated_institution: GraphObject(prefix='AFL_INS', name=NodeType.affiliated_institution.value),
    NodeType.last_institution: GraphObject(prefix='AFL_INS', name=NodeType.affiliated_institution.value),
    NodeType.issn: GraphObject(prefix='issn', name=NodeType.issn.value),
    NodeType.authorship: GraphObject(prefix='aush', name=NodeType.authorship.value),
    NodeType.year: GraphObject(prefix='YR', name=NodeType.year.value)
}