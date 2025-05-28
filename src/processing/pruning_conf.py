'''
pruning_conf.py
Configuration to help map out desired fields and types for select OpenAlex objects
'''

from enum import Enum
from polars import DataFrame
import polars as pl

class ObjectFields(Enum):
    # For the institution object
    institution = (
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
        'x_concepts',
    )


class PruningFunction:
    def pruneAuthors(data: DataFrame):
        pass

    def pruneFunders(data: DataFrame):
        pass

    def pruneSources(data: DataFrame):
        pass

    def pruneWorks(data: DataFrame):
        pass

    def pruneInstitutions(data: DataFrame):
        data = data\
            .select(ObjectFields.institution.value)
        
    
    pruning_functions = {
        'authors' : pruneAuthors,
        'funders' : pruneFunders,
        'institutions' : pruneInstitutions,
        'sources' : pruneSources,
        'works' : pruneWorks,
    }

    def __init__(self, identifier: str):
        self.prune = self.pruning_functions.get(identifier, (lambda x: x))

def pruneData(identifier : str, data: DataFrame):
    pruning_fx = PruningFunction(identifier)
    print(pruning_fx.prune.__name__)
    pruning_fx.prune(data)
