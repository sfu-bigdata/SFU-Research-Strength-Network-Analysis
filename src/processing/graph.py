import polars as pl
from polars import LazyFrame
from typing import Optional
from .conf import GraphTable, GraphRelationship
from config import NodeType

class Relationships():
    
    RelationshipTypeMap = {
        (NodeType.author, NodeType.work) : 'AUTHORSHIPS',
        (NodeType.institution, NodeType.institution) : 'PARENT_OF',
        (NodeType.institution, NodeType.author) : 'AFFILIATED_WITH',
        (NodeType.institution, NodeType.source) : 'HOSTS',
        (NodeType.institution, NodeType.funder) : 'CAN_ALSO_BE',
        (NodeType.institution, NodeType.publisher) : 'CAN_ALSO_BE',
        (NodeType.institution, NodeType.work) : 'AUTHORSHIPS',
        (NodeType.funder, NodeType.work) : 'FUNDS',
        (NodeType.publisher, NodeType.source) : 'HOSTS',
        (NodeType.source, NodeType.work) : 'CONTAINS',
        (NodeType.work, NodeType.topic) : 'HAS_TOPIC',
        (NodeType.source, NodeType.topic) : 'HAS_TOPIC',
        (NodeType.topic, NodeType.topic) : 'IN_SUBFIELD',
        (NodeType.subfield, NodeType.field) : 'IN_FIELD',
        (NodeType.field, NodeType.domain) : 'IN_DOMAIN',
        (NodeType.author, NodeType.last_institution) : 'LAST_AFFILIATED_WITH'
    }

    def calculate_relationship(self, left: NodeType, right: NodeType) -> Optional[str]:

        if (left, right) in self.RelationshipTypeMap:
            return self.RelationshipTypeMap[(left, right)]
        elif (right, left) in self.RelationshipTypeMap:
            return self.RelationshipTypeMap[(right, left)]
        else:
            return None
        
def relationship(
        ldataframe: LazyFrame,
        rdataframe: LazyFrame,
        relationshipType: Optional[str],
        l_id : str = 'id',
        r_id : str = 'id',
) -> LazyFrame:
    '''
        Return a dataframe resolving the relationships between two dataframes.
        @param ldataframe The left dataframe to be supplied
        @param rdataframe The right dataframe to be supplied
        @param in_common Whether or not relationships should use common fields
        @param relationships Explicitly defined relationships mapping fields
    '''

    if relationshipType is None:
        raise ValueError('No defined relationship type found.')
    

    res = dict()
    lschema, rschema = ldataframe.collect_schema().names(), rdataframe.collect_schema().names()

    if not (l_id in lschema and r_id in rschema):
        if l_id not in lschema and r_id not in rschema:
            raise(f'ID: {l_id} not found in left dataframe, {r_id} not found in right dataframe')
        if l_id not in lschema:
            raise(f'ID: {l_id} not found in left dataframe.')
        if r_id not in rschema:
            raise(f'ID: {r_id} not found in right dataframe.')
    
    # Iterate through the lazyframes
    '''
    res[':START_ID'] = ldataframe.get_column(l_id).to_list()
    res[':END_ID'] = rdataframe.get_column(r_id).to_list()
    res[':TYPE'] = relationshipType if relationshipType else ''

    return DataFrame(data=res, schema=sorted(res.keys()))
    '''
    # Temporary Indexes
    ldataframe_idx = ldataframe.with_columns(
        pl.int_range(0, pl.len()).alias('_idx')
    )

    rdataframe_idx = rdataframe.with_columns(
        pl.int_range(0, pl.len()).alias('_idx')
    )

    combined = ldataframe_idx.join(
        other=rdataframe_idx,
        how="inner",
        on="_idx"
    )

    relationshipTable = combined.select(
        pl.col(l_id).alias(':START_ID'),
        pl.col(r_id).alias(':END_ID'),
        pl.lit(relationshipType).alias(':TYPE')
    )

    return relationshipTable


# Provided a collection of data, output relationship tables and node tables
def generateGraphNodes(
    data: list[GraphTable]
) -> tuple[list[GraphTable], list[GraphTable]]:
    # Iterate through all the tables comparing them against one another
    relationshipTables = []
    relationships = Relationships()

    for first in data[:-1]:
        for second in data[1:]:
            relationshipType = relationships.calculate_relationship(first.type, second.type)
            if relationshipType is not None:
                rtable = relationship(first.data, second.data, relationshipType)
                relationshipTables.append(GraphTable(name=first.name+'_'+second.name+'_relationship',
                                                     type=NodeType.relationship, 
                                                     data=rtable))
    
    return (data, relationshipTables)

def includeRelationshipType(
        relationship: GraphRelationship
):
    
    if (pair := (relationship.start_type, relationship.target_type) in Relationships.RelationshipTypeMap):
        relationship.data = relationship.data.with_columns(
            pl.lit(Relationships.RelationshipTypeMap[pair]).alias(":TYPE")
        )
    else:
        raise TypeError(f'Unable to find relationship map for types: {pair}')

