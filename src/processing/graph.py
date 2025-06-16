'''
'''
from polars import DataFrame
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
        ldataframe: DataFrame,
        rdataframe: DataFrame,
        l_id : str,
        r_id : str,
        relationshipType: Optional[str]
) -> DataFrame:
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
    res[':START_ID'] = ldataframe.get_column(l_id).to_list()
    res[':END_ID'] = rdataframe.get_column(r_id).to_list()
    res[':TYPE'] = relationshipType if relationshipType else ''

    return DataFrame(data=res, schema=sorted(res.keys()))
