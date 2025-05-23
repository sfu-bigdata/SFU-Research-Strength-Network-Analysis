from enum import Enum
import os.path as path
BASE_URI = 'https://api.openalex.org/'
MAXIMUM_RESULTS_BASIC_PAGINATION = 10000
OUTPUT_RAW_DATA_DIR = path.join('data', 'raw')

class APIEndpoints(Enum):
    WORKS = '%s%s' % (BASE_URI, 'works')
    AUTHORS = '%s%s' % (BASE_URI, 'authors')
    SOURCES = '%s%s' % (BASE_URI, 'sources')
    INSTITUTIONS = '%s%s' % (BASE_URI, 'institutions')
    TOPICS = '%s%s' % (BASE_URI, 'topics')
    KEYWORDS = '%s%s' % (BASE_URI, 'keywords')
    PUBLISHERS = '%s%s' % (BASE_URI, 'publishers')
    FUNDERS = '%s%s' % (BASE_URI, 'funders')

class PaginationTypes(Enum):
    BASIC = 0
    CURSOR = 1

class QueryParams(Enum):
    items_per_page = 'per-page'
    cursor_pagination = 'cursor'
    page = 'page'
    filter = 'filter'
    search = 'search'
    group = 'group_by'
    sort = 'sort'

'''
def generate_parameter(type: QueryParams, value) -> Optional[str]:
    if type not in QueryParams:
        raise Exception('Unsupported parameter type: {type}, {value}')
    return '%s=%s' % (QueryParams.value, value)
'''