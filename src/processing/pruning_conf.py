'''
pruning_conf.py
Configuration to help map out desired fields and types for select OpenAlex objects
'''

from enum import Enum
from polars import LazyFrame
import polars as pl
from .conf import GraphTable, GraphRelationship, GraphDataCollection
from typing import Iterator
from config import NodeType, TableMap, GRAPH_START_ID, GRAPH_END_ID

class ObjectFields(Enum):
    # For the institution object
    institution= (
        'id',
        'display_name',
        'lineage',
        'works_count',
        'summary_stats',
        'counts_by_year',
        #'topics',
        'topic_share',
        'associated_institutions'
    )

    author = (
        'affiliations',
        'display_name',
        'cited_by_count',
        'id',
        'last_known_institutions',
        'summary_stats',
        'works_count',
        'topics',
    )

    funder = (
        'display_name',
        'cited_by_count',
        'country_code',
        'counts_by_year',
        'grants_count',
        'id',
        'summary_stats',
        'works_count'
    )

    source = (
        'apc_usd',
        'cited_by_count',
        'country_code',
        'counts_by_year',
        'display_name',
        'host_organization',
        'id',
        'is_core',
        'is_in_doaj',
        'is_oa',
        'summary_stats',
        'type',
        'works_count',
        'topics'
    )

    work = (
        'authorships',
        'apc_paid',
        'display_name',
        'citation_normalized_percentile',
        'cited_by_count',
        'countries_distinct_count',
        'counts_by_year',
        'fwci',
        #'grants',
        'id',
        'institutions_distinct_count',
        'publication_date',
        #'referenced_works',
        'topics',
        'type',
        'open_access'
    )

    publisher = (
        'display_name',
        'cited_by_count',
        'country_codes', # Tentative
        'counts_by_year',
        'display_name',
        'id',
        'lineage',
        'summary_stats',
        'works_count',
    )

    topic = (
        'id',
        'display_name',
        'subfield',
        'field',
        'domain'
    )

NodeTypeToFields = {
    NodeType.SFU_U15_institution: ObjectFields.institution,
    NodeType.author: ObjectFields.author,
    NodeType.funder: ObjectFields.funder,
    NodeType.source: ObjectFields.source,
    NodeType.publisher: ObjectFields.publisher,
    NodeType.topic: ObjectFields.topic,
    NodeType.work: ObjectFields.work
}
from typing import Mapping
def is_composite(item) -> bool:
    return isinstance(item, list) or isinstance(item, Mapping)

def check_and_extract_url(expr: pl.Expr)-> pl.Expr:
    url_pattern = r'https?://(?:openalex|ror)\.\S+'

    return (
        pl.when(
            expr.str.contains(url_pattern)
        )
        .then(
            expr.str.split('/').list.last()
        )
        .otherwise(
            expr
        )
    )

def clean_nested_string(expr: pl.Expr, type: pl.DataType) -> pl.Expr:
    if type == pl.String:
        return check_and_extract_url(expr)
    
    elif isinstance(type, pl.List):
        return expr.list.eval(
            clean_nested_string(pl.element(), type.inner)
        )

    elif isinstance(type, pl.Struct):
        transforms = []
        for field in type.fields:
            struct_expr = clean_nested_string(
                expr.struct.field(field.name),
                field.dtype
            ).alias(field.name)

            transforms.append(struct_expr)

        return pl.struct(transforms)

    else:
        return expr

def process_strings(data: LazyFrame) -> LazyFrame:
    return data.with_columns(
        [clean_nested_string(pl.col(name), dtype).alias(name) for name, dtype in data.collect_schema().items()]
    )


class PruningFunction:

    '''
    # Add column describing the role of the institution
    '''
    columnsFunction = {
        
        'lineage': [
                    pl.when(pl.col('lineage').list.len() > 1)
                    .then(False)
                    .otherwise(True)
                    .alias('lineage_root'),
        ],

        # Explode summary stats
        'summary_stats': [
            pl.col('summary_stats').struct.unnest()
        ],

        # Ensure nulls are filled
        'grants_count': [
            pl.col('grants_count').fill_null(0)
        ],
        
        'country_code': [
            pl.col('country_code').fill_null('null')
        ],
    
        'apc_usd': [
            pl.col('apc_usd').fill_null(0)
        ],
    
        'apc_paid': [
            pl.col('apc_paid').struct.field('value_usd').alias('apc_paid')
        ],

        'citation_normalized_percentile': [
            pl.col('citation_normalized_percentile').struct.field('value').alias('citation_normalized_percentile')
        ],

        'open_access': [
            pl.col('open_access').struct.field(['is_oa', 'oa_status'])
        ]
    }
        
    columnsToDrop = {'open_access', 'summary_stats'}

    def targetedManipulateFields(self, data: LazyFrame) -> LazyFrame:
        schema = set(data.collect_schema().names())
        in_common = set(self.columnsFunction.keys()).intersection(schema)

        added_data = data.with_columns(
            [item for col in in_common for item in self.columnsFunction.get(col)]
        )
        
        to_drop = self.columnsToDrop.intersection(set(added_data.collect_schema().names()))
        
        return added_data.drop(to_drop)
    
    def prunePublishers(self, data: LazyFrame) -> LazyFrame:
        data = data\
            .select(ObjectFields.publisher.value)        
        return data
    
    def pruneAuthors(self, data: LazyFrame) -> LazyFrame:
        data = data\
            .select(ObjectFields.author.value)

        return data


    def pruneFunders(self, data: LazyFrame) -> LazyFrame:
        data = data\
            .select(ObjectFields.funder.value)
        
        return data

    def pruneSources(self, data: LazyFrame) -> LazyFrame:
        data = data\
            .select(ObjectFields.source.value)
        

        return data

    def pruneWorks(self, data: LazyFrame) -> LazyFrame:
        data = data\
            .select(ObjectFields.work.value)\
        
        return data
    
    def pruneInstitutions(self, data: LazyFrame) -> LazyFrame:
        data = data\
            .select(ObjectFields.institution.value)\
            
        return data
    
    def pruneTopics(self, data:LazyFrame) -> LazyFrame:
        data = data\
            .select(ObjectFields.topic.value)
        
        return data
    
    pruning_functions = {
        NodeType.author: pruneAuthors,
        NodeType.funder: pruneFunders,
        NodeType.SFU_U15_institution: pruneInstitutions,
        NodeType.source: pruneSources,
        NodeType.work: pruneWorks,
        NodeType.topic: pruneTopics,
    }

    def __init__(self, nodeType: NodeType):
        self.prune = self.pruning_functions.get(nodeType, (lambda x: x))
        self.nodeType = nodeType
    
    def __call__(self, data: LazyFrame) -> tuple[LazyFrame, NodeType]:
        data = self.prune(self, data)
        data = self.targetedManipulateFields(data)
        data = process_strings(data)
        
        data = data.with_columns(
            pl.selectors.numeric().fill_null(0),
            pl.selectors.string().fill_null(''),
        )
        
        data = data.unique(subset=['id'], keep='first')

        return (data, self.nodeType)
    
class SecondaryInformation():
    '''
        Derive additional tables from supplied data tables (topics, affiliations, etc)
    '''
    def _deriveTopicHierarchy(originaTable: GraphTable, data: LazyFrame) -> GraphDataCollection:

        subfield_nodes = data.select(
            pl.col('subfield')
        ).unnest('subfield')\
        .drop_nulls()\
        .unique(keep='first', subset=['id'])

        field_nodes = data.select(
            pl.col('field')
        ).unnest('field')\
        .drop_nulls()\
        .unique(keep='first', subset=['id'])
        
        domain_nodes = data.select(
            pl.col('domain')
        ).unnest('domain')\
        .drop_nulls()\
        .unique(keep='first', subset=['id'])

        topic_subfield_rl = data.select(
            pl.col('id').alias(GRAPH_START_ID),
            pl.col('subfield').struct.field('id').alias(GRAPH_END_ID)
        ).drop_nulls().unique()

        subfield_field_rl = data.select(
            pl.col('subfield').struct.field('id').alias(GRAPH_START_ID),
            pl.col('field').struct.field('id').alias(GRAPH_END_ID)    
        ).drop_nulls().unique()

        field_domain_rl = data.select(
            pl.col('field').struct.field('id').alias(GRAPH_START_ID),
            pl.col('domain').struct.field('id').alias(GRAPH_END_ID)
        ).drop_nulls().unique()

        nodes = [
            GraphTable(NodeType.field.value, NodeType.field, field_nodes),
            GraphTable(NodeType.subfield.value, NodeType.subfield, subfield_nodes),
            GraphTable(NodeType.domain.value, NodeType.domain, domain_nodes)
        ]

        relationships = [
            GraphRelationship(topic_subfield_rl, start_type=NodeType.topic, target_type=NodeType.subfield),
            GraphRelationship(subfield_field_rl, start_type=NodeType.subfield, target_type=NodeType.field),
            GraphRelationship(field_domain_rl, start_type=NodeType.field, target_type=NodeType.domain)
        ]

        originaTable.data = data.select(
            pl.col('id'),
            pl.col('display_name')
        )

        return GraphDataCollection(
            nodes=nodes,
            relationships=relationships
            )

    def _deriveTopicRelationships(originalTable: GraphTable, data: LazyFrame) -> GraphDataCollection:
        fields = NodeTypeToFields[originalTable.type].value

        select = []
        if 'topics' in fields:
            select.append('topics')
        if 'topic_share' in fields:
            select.append('topic_share')

        exploded_topics = data.select(
            pl.col(select),
            pl.col('id').alias(GRAPH_START_ID)
        )\
        .explode(select)\
        .unnest(select)

        exploded_topics = exploded_topics.drop(['display_name', 'subfield', 'field', 'domain'])

        data_topic = exploded_topics.rename({
            'id':GRAPH_END_ID
        }).unique().drop_nulls()

        relationships = [
            GraphRelationship(data_topic, originalTable.type, NodeType.topic)
        ]

        originalTable.data = originalTable.data.drop(select)
        return GraphDataCollection(relationships=relationships, nodes=[])
    
    def _deriveAffiliatedInstitutions(originalTable: GraphTable, data:LazyFrame) -> GraphDataCollection:
        
        exploded_institutions = data.select(
            pl.col('id'),
            pl.col('affiliations')
        )\
            .explode('affiliations')
        
        affiliations = exploded_institutions.select(
            pl.col('affiliations').struct.unnest(),
            pl.col('id')
        )

        institutions_nodes = affiliations.select(
            pl.col('institution').struct.field('id'),
            pl.col('institution').struct.field('display_name'),
            pl.col('institution').struct.field('type')
        ).drop_nulls().unique(keep='first', subset=['id'])

        author_institution_rl = affiliations.select(
            pl.col('id').alias(GRAPH_START_ID),
            pl.col('institution').struct.field('id').alias(GRAPH_END_ID),
            pl.col('years')
        ).drop_nulls().unique()

        institutions_geo_rl = affiliations.select(
            pl.col('institution').struct.field('id').alias(GRAPH_START_ID),
            pl.col('institution').struct.field('country_code').alias(GRAPH_END_ID)
        ).drop_nulls().unique()

        # Get the last known institutions and create a relationship table from them, no need to add institution as they should be included in affiliations
        last_known = data.select(
            pl.col('id'),
            pl.col('last_known_institutions')
        ).explode('last_known_institutions')

        author_last_known_rl = last_known.select(
            pl.col('id').alias(GRAPH_START_ID),
            pl.col('last_known_institutions').struct.field('id').alias(GRAPH_END_ID)
        )

        originalTable.data = originalTable.data.drop(['affiliations', 'last_known_institutions'])

        return GraphDataCollection(relationships=[
            GraphRelationship(author_institution_rl, start_type=NodeType.author, target_type=NodeType.affiliated_institution),
            GraphRelationship(institutions_geo_rl, start_type=NodeType.affiliated_institution, target_type=NodeType.geographic),
            GraphRelationship(data=author_last_known_rl, start_type=NodeType.author, target_type=NodeType.last_institution)
        ], nodes=[
            GraphTable(name=NodeType.affiliated_institution.value, type=NodeType.affiliated_institution, data=institutions_nodes)
        ])

    def _deriveLineage(originalTable: GraphTable, data:LazyFrame) -> GraphDataCollection:
        '''
        Return a relationship table with 
        base_institution_id -> reference institution id
        related_institution_id -> another insituttion in the lineage
        lineage_root -> boolean; whether or not base is root
        relationship -> relationship between base and related lineage institutions
        '''
        exploded_lineage = data.select(
            pl.col('id').alias('base_institution_id'),
            pl.col('lineage').alias('related_institution_id'),
            pl.col('lineage_root'),
            pl.col('associated_institutions')
        ).explode('related_institution_id')\
        .filter(
            pl.col('base_institution_id') != pl.col('related_institution_id')
        )

        lineage_rl = exploded_lineage\
            .explode('associated_institutions')\
            .filter(
                pl.col('associated_institutions').struct.field('id') == pl.col('related_institution_id')
            )\
            .select(
                pl.col('base_institution_id').alias(GRAPH_START_ID),
                pl.col('related_institution_id').alias(GRAPH_END_ID),
                pl.col('associated_institutions').struct.field('relationship')
            )
        
        # Map the SFU_15 to the its related institution node        
        affiliated_rl = data.select(
            pl.col('id').alias(GRAPH_START_ID)
        )\
        .with_columns(
            pl.col(GRAPH_START_ID).alias(GRAPH_END_ID)
        )

        originalTable.data = originalTable.data.drop(['lineage', 'associated_institutions'])        
        return GraphDataCollection(
            relationships=[
                GraphRelationship(data=lineage_rl, start_type=NodeType.SFU_U15_institution, target_type=NodeType.SFU_U15_institution),
                GraphRelationship(data=affiliated_rl, start_type=NodeType.affiliated_institution, target_type=NodeType.SFU_U15_institution)
            ],
            nodes=[]
        )

    def _deriveAuthorships(originalTable: GraphTable, data:LazyFrame) -> GraphDataCollection:
        # The works data is large, so minimize the data size
        exploded = data.select(
            pl.col('id').alias('work_id'),
            pl.col('authorships')
        ).explode('authorships')\
        .unnest('authorships')\
        .select(
            pl.col('work_id'),
            pl.col('author').struct.field('id').alias('author_id'),
            pl.col('author').struct.field('display_name'),
            pl.col('institutions')
        )\
        .explode('institutions')

        '''
        All authors that had relation to the U15+SFU are contained within the AUTHORS table
        Best to leave out unless want to include another degree of separation to include non-target authors.

        authorship_rl = exploded.select(
            pl.col('author_id'),
            pl.col('institutions').struct.field('id').alias('institution_id'),
        ).drop_nulls().unique()
        '''

        institutions_nodes = exploded.select(
            pl.col('institutions').struct.field('id'),
            pl.col('institutions').struct.field('display_name'),
            pl.col('institutions').struct.field('type')
        ).drop_nulls().unique(keep='first', subset=['id'])

        institution_geo_rl = exploded.select(
            pl.col('institutions').struct.field('id').alias(GRAPH_START_ID),
            pl.col('institutions').struct.field('country_code').alias(GRAPH_END_ID)
        ).drop_nulls().unique()

        # Create relationship table mapping work to an author
        author_work_rl = exploded.select(
            pl.col('author_id').alias(GRAPH_START_ID),
            pl.col('work_id').alias(GRAPH_END_ID)
        )

        originalTable.data = originalTable.data\
            .drop('authorships')

        return GraphDataCollection(
            nodes=[
                GraphTable(name=NodeType.affiliated_institution.value, type=NodeType.affiliated_institution, data=institutions_nodes)
            ],
            relationships=[
                GraphRelationship(data=institution_geo_rl, start_type=NodeType.affiliated_institution, target_type=NodeType.geographic),
                GraphRelationship(data=author_work_rl, start_type=NodeType.author, target_type=NodeType.work)
            ]
        )

    derivedTables = {
        NodeType.source: [NodeType.topic],
        NodeType.author: [NodeType.topic, NodeType.affiliated_institution],
        NodeType.SFU_U15_institution: [NodeType.topic, NodeType.related_institution],
        NodeType.work : [NodeType.authorship, NodeType.topic],
        NodeType.topic : [NodeType.domain]
    }

    derivedFunctions = {
        NodeType.domain: _deriveTopicHierarchy,
        NodeType.topic: _deriveTopicRelationships,
        NodeType.affiliated_institution: _deriveAffiliatedInstitutions,
        NodeType.related_institution: _deriveLineage,
        NodeType.authorship: _deriveAuthorships
    }


    def derive(self, table : GraphTable) -> list[GraphDataCollection]:
        type = table.type
        sublist : list[GraphDataCollection] = []
        
        if (secondary:= self.derivedTables.get(type, None)) is not None:
            base = table.data
            for subtable in secondary:
                newTable = base
                sublist.append(
                    self.derivedFunctions[subtable](table, newTable)
                )
            

        return sublist