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

'''
def check_and_extract_url(data: DataFrame) -> DataFrame:
    url_pattern = r'https?://(?:openalex|ror)\.\S+'
    
    string_cols = pl.col(pl.String)
    predicate = string_cols.str.contains(url_pattern)

    data = data.with_columns(
        pl.when(predicate)
            .then((string_cols.str.split('/').list.last()))
            .otherwise(string_cols)
    )

    return data 
'''

from typing import Mapping
def is_composite(item) -> bool:
    return isinstance(item, list) or isinstance(item, Mapping)

def check_and_extract_url(elements):
    url_pattern = r'https?://(?:openalex|ror)\.\S+'
    if (elements.dtype.base_type() is pl.Utf8 or elements.dtype.base_type() is pl.String)\
        and (elements.str.contains(url_pattern)).any():
        elements = elements.str.split('/').list.last()

    elif elements.dtype.is_nested():
        def process_nested(element):
            print(element)
            if isinstance(element, str):
                element = element.split('/')[-1]
            elif is_composite(element):
                if isinstance(element, list):
                    element = [process_nested(e) for e in element]
                elif isinstance(element, Mapping):
                    for k, v in element.items():
                        element[k] = process_nested(v)
            return element
        
        elements = elements.map_elements(process_nested, return_dtype=elements.dtype)
                    
    return elements

def process_strings(data : DataFrame) -> DataFrame:
    data = data.select(
        pl.all().map_batches(check_and_extract_url)
    )    
    return data

def flattenStructs(data: DataFrame):
    # Polars is having some difficulty identfying struct types with the Native API
    keys = [key for key, value in data.schema.items() if value.base_type() is pl.Struct]
    for colName in keys:
        data = data.with_columns(
            data.select(
                pl.col(colName).name.prefix_fields(
                    colName+'_'
                )
            )
        )

        data = data.unnest(colName)

    return data

class PruningFunction:
    
    def prunePublishers(data: DataFrame) -> DataFrame:
        data = data\
            .select(ObjectFields.publishers.value)
        
        return data
    
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
        #data = flattenStructs(data)
        data = process_strings(data)
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
        data = self.prune(data)
        return data