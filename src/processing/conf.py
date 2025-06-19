from enum import Enum
from dataclasses import dataclass
from polars import LazyFrame

class NodeType(Enum):
    institution = 'institution'
    author = 'author'
    funder = 'funder'
    source = 'source'
    work = 'work'
    topic = 'topic'
    journal = 'journal'
    publisher = 'publisher'
    relationship = 'relationship' 
    none = 'none'

TableMap = {          
    'authors' : NodeType.author,
    'funders' : NodeType.funder,
    'institutions' : NodeType.institution,
    'sources' : NodeType.source,
    'journals': NodeType.journal,
    'works' : NodeType.work,
    'funded_works': NodeType.work
}

@dataclass
class GraphTable:
    name: str
    type: NodeType
    data: LazyFrame