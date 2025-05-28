'''
pruning_conf.py
Configuration to help map out desired fields and types for select OpenAlex objects
'''

from enum import Enum
from polars import DataFrame
import polars as pl

class ObjectFields(Enum):
    # For the institution object
    institutions = (
        'id',
        'ror',
        'display_name',
        'lineage',
        'repositories',
        'works_count',
        'summary_stats',
        'geo',
        'counts_by_year',
        'topics',
        'topic_share',
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


class PruningFunction:
    def pruneAuthors(data: DataFrame) -> DataFrame:
        data = data\
            .select(ObjectFields.authors.value)
        
        return data

    def pruneFunders(data: DataFrame) -> DataFrame:
        data = data\
            .select(ObjectFields.funders.value)
        
        return data

    def pruneSources(data: DataFrame) -> DataFrame:
        data = data\
            .select(ObjectFields.sources.value)
        
        return data

    def pruneWorks(data: DataFrame) -> DataFrame:
        data = data\
            .select(ObjectFields.works.value)
        
        return data
    
    def pruneInstitutions(data: DataFrame) -> DataFrame:
        data = data\
            .select(ObjectFields.institutions.value)
        
        return data
    
    pruning_functions = {
        'authors' : pruneAuthors,
        'funders' : pruneFunders,
        'institutions' : pruneInstitutions,
        'sources' : pruneSources,
        'works' : pruneWorks,
    }

    def __init__(self, identifier: str):
        self.prune = self.pruning_functions.get(identifier, (lambda x: x))
    
    def __call__(self, data: DataFrame) -> DataFrame:
        return self.prune(data)