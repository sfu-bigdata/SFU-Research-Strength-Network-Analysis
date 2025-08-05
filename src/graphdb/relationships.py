import polars as pl
from polars import LazyFrame
from typing import Optional
from config import NodeType, GRAPH_START_ID, GRAPH_END_ID
from dataclasses import dataclass
from .conf import GraphObject, ObjectNames
from enum import Enum

@dataclass
class RelationshipObject:
    origin_node: GraphObject
    target_node: GraphObject
    origin_id: str
    target_id: str
    rel_type: str

class Relationships():
    
    RelationshipTypeMap = {
        (NodeType.affiliated_institution, NodeType.affiliated_institution): 'IN_LINEAGE_WITH',
        (NodeType.affiliated_institution, NodeType.geographic): 'INSTITUTION_SITUATED_IN',
        (NodeType.affiliated_institution, NodeType.SFU_U15_institution): 'INSTITUTION_IS_THE_SAME_AS',
        (NodeType.author, NodeType.affiliated_institution): 'AFFILIATED_WITH',
        (NodeType.author, NodeType.authorship): 'HAS_AUTHORSHIP',
        (NodeType.author, NodeType.last_institution) : 'LAST_AFFILIATED_WITH',
        (NodeType.author, NodeType.topic) : 'HAS_WORKS_CONCERNING',
        (NodeType.author, NodeType.work) : 'HAS_WORK',
        (NodeType.author, NodeType.year) : 'IN_YEAR',
        (NodeType.authorship, NodeType.affiliated_institution) : 'AUTHORSHIP_HAS_AFFILIATION_WITH_INSTITUTION',
        (NodeType.authorship, NodeType.work): 'AUTHORSHIP_ON_WORK',
        (NodeType.field, NodeType.domain) : 'IN_DOMAIN',
        (NodeType.funder, NodeType.SFU_U15_institution) : 'IS_A_ROLE_OF',
        (NodeType.funder, NodeType.work) : 'FUNDS',
        (NodeType.funder, NodeType.year) : 'IN_YEAR',
        (NodeType.publisher, NodeType.source) : 'HOSTS',
        (NodeType.SFU_U15_institution, NodeType.author) : 'AFFILIATED_WITH',
        (NodeType.SFU_U15_institution, NodeType.funder) : 'CAN_ALSO_BE',
        (NodeType.SFU_U15_institution, NodeType.geographic): 'SITUATED_IN',
        (NodeType.SFU_U15_institution, NodeType.publisher) : 'CAN_ALSO_BE',
        (NodeType.SFU_U15_institution, NodeType.SFU_U15_institution) : 'IN_LINEAGE_WITH',
        (NodeType.SFU_U15_institution, NodeType.source) : 'HOSTS',
        (NodeType.SFU_U15_institution, NodeType.topic) : 'HAS_WORKS_CONCERNING',
        (NodeType.SFU_U15_institution, NodeType.year) : 'IN_YEAR',
        (NodeType.source, NodeType.issn) : 'HAS_ISSN',
        (NodeType.source, NodeType.topic) : 'HAS_TOPIC',
        (NodeType.source, NodeType.year) : 'IN_YEAR',
        (NodeType.subfield, NodeType.field) : 'IN_FIELD',
        (NodeType.topic, NodeType.subfield) : 'IN_SUBFIELD',
        (NodeType.work, NodeType.issn) : 'HOSTED_IN_ISSN',
        (NodeType.work, NodeType.topic) : 'HAS_TOPIC',
        (NodeType.work, NodeType.work) : 'REFERENCES_WORK',
        (NodeType.work, NodeType.year) : 'IN_YEAR'
    }

    def createRelationshipObject(
        self,
        start_node_type : NodeType,
        end_node_type : NodeType,
        start_cols=GRAPH_START_ID,
        target_cols=GRAPH_END_ID
    ):
        
        rel_type = self.RelationshipTypeMap.get((start_node_type, end_node_type), None)
        
        if rel_type is None:
            raise(f'Error Relationship Type not found for nodes: {start_node_type.name} -> {end_node_type.name}')

        return RelationshipObject(
            origin_node=ObjectNames[start_node_type],
            target_node=ObjectNames[end_node_type],
            origin_id=start_cols,
            target_id=target_cols,
            rel_type=rel_type
        )

class PropertyType(Enum):
    ONE_TO_ONE=0,
    CONTAINED=1

@dataclass
class PropertyRelationship:
    relationship: RelationshipObject
    properties: dict[str, str]
    propertyType: PropertyType

