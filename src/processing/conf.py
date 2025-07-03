from dataclasses import dataclass
from polars import LazyFrame
from typing import Iterable
from config import NodeType

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

designatedDirectories = {
    'authors': NodeType.author,
    'funders': NodeType.funder,
    'institutions': NodeType.SFU_U15_institution,
    'sources': NodeType.source,
    'works': NodeType.work,
    'topics': NodeType.topic
}