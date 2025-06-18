'''
pruning_conf.py
Configuration to help map out desired fields and types for select OpenAlex objects
'''

from enum import Enum
from polars import LazyFrame
import polars as pl

class ObjectFields(Enum):
    # For the institution object
    institutions = (
        'id',
        'display_name',
        'lineage',
        #'repositories',
        'works_count',
        'summary_stats',
        'geo',
        'counts_by_year',
        'topics',
    )

    authors = (
        'affiliations',
        'cited_by_count',
        'counts_by_year',
        'id',
        'last_known_institutions',
        'summary_stats',
        'works_count',
        'topics',
    )

    funders = (
        'cited_by_count',
        'country_code',
        'counts_by_year',
        'grants_count',
        'id',
        'summary_stats',
        'works_count'
    )

    sources = (
        'apc_usd',
        'cited_by_count',
        'country_code',
        'counts_by_year',
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

    works = (
        'authorships',
        'apc_list',
        'apc_paid',
        'biblio', # Tentative
        'citation_normalized_percentile',
        # 'cited_by_api_url', # Could potentially be used later to gather information on citations
        'cited_by_count',
        'concepts',
        'corresponding_author_ids',
        'corresponding_institution_ids',
        'countries_distinct_count',
        'counts_by_year',
        'fwci',
        'grants',
        'id',
        'institutions_distinct_count',
        'keywords',
        'language',
        'locations',
        'publication_date',
        'referenced_works',
        'topics',
        'type',
        'open_access'
    )

    publishers = (
        'cited_by_count',
        'country_codes', # Tentative
        'counts_by_year',
        'display_name',
        'id',
        'lineage',
        'summary_stats',
        'works_count',
    )

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

                    # Replace lineage with a hash
                    pl.col('lineage').list.sort()
                    .list.join('')
                    .hash()
                    .cast(pl.String)
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

        'open_access': [
            pl.col('open_access').struct.field(['is_oa', 'oa_status'])
        ]
    }
        
    columnsToDrop = {'open_access'}

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
            .select(ObjectFields.publishers.value)        
        return data
    
    def pruneAuthors(self, data: LazyFrame) -> LazyFrame:
        data = data\
            .select(ObjectFields.authors.value)

        return data


    def pruneFunders(self, data: LazyFrame) -> LazyFrame:
        data = data\
            .select(ObjectFields.funders.value)
        
        return data

    def pruneSources(self, data: LazyFrame) -> LazyFrame:
        data = data\
            .select(ObjectFields.sources.value)
        

        return data

    def pruneWorks(self, data: LazyFrame) -> LazyFrame:
        data = data\
            .select(ObjectFields.works.value)\
        
        return data
    
    def pruneInstitutions(self, data: LazyFrame) -> LazyFrame:
        data = data\
            .select(ObjectFields.institutions.value)\
            
        return data
    

    pruning_functions = {
        'authors' : pruneAuthors,
        'funders' : pruneFunders,
        'institutions' : pruneInstitutions,
        'sources' : pruneSources,
        'journals': pruneSources,
        'works' : pruneWorks,
        'funded_works': pruneWorks
    }

    def __init__(self, identifier: str):
        self.prune = self.pruning_functions.get(identifier, (lambda x: x))
    
    def __call__(self, data: LazyFrame) -> LazyFrame:
        data = self.prune(self, data)
        data = self.targetedManipulateFields(data)
        data = process_strings(data)
        data = data.with_columns(
            pl.selectors.numeric().fill_null(0),
            pl.selectors.string().fill_null('')
        )

        return data