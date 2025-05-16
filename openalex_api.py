'''
openalex-api.py
Class containing relevant values and methods for OpenAlex API interaction
'''

# IDs for relevant institutions SFU + U15
institution_ids = (
    'i18014758', # Simon Fraser University
    'i141945490', # University of British Columbia
    'i129902397', # Dalhousie University
    'i154425047', # University of Alberta
    'i5023651', # McGill University
    'i98251732', # McMaster University
    'i125749732', # Western University
    'i151746483', # University of Waterloo
    'i70931966', # Université de Montréal
    'i43406934', # Université Laval
    'i185261750', # University of Toronto
    'i168635309', # University of Calgary
    'i153718931', # University of Ottawa
    'i204722609', # Queen's University
    'i46247651', # University of Manitoba
    'i32625721', # University of Saskatchewan
)

BASE_URI = 'https://api.openalex.org/'
from enum import Enum
from typing import Optional
class APIEndpoints(Enum):
    WORKS = '%s%s' % (BASE_URI, 'works')
    AUTHORS = '%s%s' % (BASE_URI, 'authors')
    SOURCES = '%s%s' % (BASE_URI, 'sources')
    INSTITUTIONS = '%s%s' % (BASE_URI, 'institutions')
    TOPICS = '%s%s' % (BASE_URI, 'topics')
    KEYWORDS = '%s%s' % (BASE_URI, 'keywords')
    PUBLISHERS = '%s%s' % (BASE_URI, 'publishers')
    FUNDERS = '%s%s' % (BASE_URI, 'funders')

class paginationTypes(Enum):
    BASIC = 0
    CURSOR = 1

import httpx
class OpenAlexApi(object):
    '''
        Singleton class providing interactive functionality with the OpenAlex API
    '''
    def __new__(cls):
        if not hasattr(cls, 'instance'):
            cls.instance = super(OpenAlexApi, cls).__new__(cls)
        return cls.instance

    def retrieve_single(self,
                    endpoint: APIEndpoints,
                    id : Optional[str] = None
                    ):
        '''
            Sends a get request for a single object of the corresponding endpoint. If no id is supplied, a random object will be requested.
            :param id -- The id of the requested object
        '''
        with httpx.Client() as client:
            url = '%s/%s'%(endpoint.value, id) if id else '%s/%s'%(endpoint.value, 'random')
            res = client.get(url)
        return res
    
    def retrieve_list(self,
                    endpoint: APIEndpoints,
                    pagination = False,
                    items_per_page = 50,
                    pagination_type = paginationTypes.BASIC,
                    filter = Optional[str],
                    search = Optional[str],
                    group = Optional[str],
                    sort = Optional[bool]
                    ) -> httpx.Response:
        '''
            Send get request to OpenAlex, anticipating a corresponding JSON response for the selected endpoint
            
            :param endpoint -- The api endpoint for desired OpenAlex object 
            :param filter -- Optional filter parameter on the get request, by default not used
            :param search -- Optional parameter that will retrieve results that contain the parameter in the title, abstract or fulltext 
            :param group  -- Optional parameter that will group results by provided attributes
        '''
        with httpx.Client() as client:
            res = client.get(endpoint.value)
        return res

api = OpenAlexApi()