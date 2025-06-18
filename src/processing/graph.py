'''
'''
import polars as pl
from polars import LazyFrame
from typing import Optional
from enum import Enum


class NodeType(Enum):
    institution = 'institution'
    author = 'author'
    funder = 'funder'
    source = 'source'
    work = 'work'
    topic = 'topic'
    publisher = 'publisher'
    none = 'none'

class Relationships():
    
    RelationshipTypeMap = {
        (NodeType.author, NodeType.work) : 'authorships',
        (NodeType.institution, NodeType.institution) : 'parent of',
        (NodeType.institution, NodeType.author) : 'affiliated with',
        (NodeType.institution, NodeType.source) : 'hosts',
        (NodeType.institution, NodeType.funder) : 'can also be',
        (NodeType.institution, NodeType.publisher) : 'can also be',
        (NodeType.institution, NodeType.work) : 'authorships',
        (NodeType.funder, NodeType.work) : 'funds',
        (NodeType.publisher, NodeType.source) : 'hosts',
        (NodeType.source, NodeType.work) : 'contains',
        (NodeType.work, NodeType.topic) : 'affiliated with',
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
        l_id : str,
        r_id : str,
        relationshipType: Optional[str]
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