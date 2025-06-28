from enum import Enum
from dataclasses import dataclass
from polars import LazyFrame
from typing import Iterable

class NodeType(Enum):
    institution = 'institution'
    author = 'author'
    funder = 'funder'
    source = 'source'
    work = 'work'
    journal = 'journal'
    publisher = 'publisher'
    relationship = 'relationship'
    topic = 'topic'
    subfield = 'subfield'
    field = 'field'
    domain = 'domain'
    affiliated_institution = 'affiliated_institution'
    none = 'none'

TableMap = {          
    'authors' : NodeType.author,
    'funders' : NodeType.funder,
    'institutions' : NodeType.institution,
    'sources' : NodeType.source,
    'journals': NodeType.source,
    'works' : NodeType.work,
    'funded_works': NodeType.work,
    'topics': NodeType.topic,
    'subfields': NodeType.subfield,
    'fields': NodeType.field,
    'domains': NodeType.domain
}


@dataclass
class GraphRelationship:
    data : LazyFrame
    start_type: NodeType
    target_type: NodeType

@dataclass
class GraphTable:
    name: str
    type: NodeType
    data: LazyFrame

@dataclass
class GraphDataCollection:
    relationships: Iterable[GraphRelationship]
    nodes: Iterable[GraphTable]