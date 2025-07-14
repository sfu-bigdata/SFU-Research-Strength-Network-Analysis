'''
openalex-api.py
Class containing relevant values and methods for OpenAlex API interaction
'''
from time import sleep
import httpx
from httpx_retries import RetryTransport
from .conf import APIEndpoints, PaginationTypes, QueryParams, MAXIMUM_RESULTS_BASIC_PAGINATION
from typing import Protocol, Optional

class id_format(Protocol):
    institution_id: str
    # publisher_id: str   # No publisher ID for SFU; using hosted repository information instead.
    funder_id: str

# IDs for relevant institutions SFU + U15
institution_ids : tuple[id_format] = (
    ('i18014758', 'f4320322551'), # Simon Fraser University
    ('i141945490', 'f4320323180'), # University of British Columbia
    ('i129902397', 'f4320321629'), # Dalhousie University
    ('i154425047', 'f4320319946'), # University of Alberta
    ('i5023651', 'f4320310638'), # McGill University
    ('i98251732', 'f4320311526'), # McMaster University
    ('i125749732', 'f4320322601'), # Western University
    ('i151746483', 'f4320322676'), # University of Waterloo
    ('i70931966', 'f4320323175'), # Université de Montréal
    ('i43406934', 'f4320310137'), # Université Laval
    ('i185261750', 'f4320322015'), # University of Toronto
    ('i168635309', 'f4320310537'), # University of Calgary
    ('i153718931', 'f4320310630'), # University of Ottawa
    ('i204722609', 'f4320321832'), # Queen's University
    ('i46247651', 'f4320311952'), # University of Manitoba
    ('i32625721', 'f4320310865'), # University of Saskatchewan
)


def send_request(client: httpx.Client, method : str, endpoint : APIEndpoints, parameters : map):
    request = client.build_request(method=method, url=endpoint.value, params=parameters)
    res = client.send(request)
    if res.status_code != 200:
        raise Exception(f'Error completing GET request:\nStatus Code: {res.status_code}\n{request}', request)
    return res

def update_cursor(client: httpx.Client, next_cursor: str, endpoint: APIEndpoints, parameters: map) -> httpx.Response:
 # Slight delay before sending the requests to respect the API
    parameters[QueryParams.cursor_pagination.value] = next_cursor
    sleep(0.1)
    return (send_request(client, 'GET', endpoint, parameters))


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
                    items_per_page = 200,
                    pagination_type = PaginationTypes.BASIC,
                    pages_count : Optional[int] = None,
                    page : Optional[int] = None,
                    filter : Optional[str] = None,
                    search : Optional[str] = None,
                    group : Optional[str] = None,
                    sort : Optional[bool] = None,
                    select: Optional[str] = None,
                    WriteFx : Optional[object] = None,
                    write_chunk_cutoff = 100
                    ) -> Optional[httpx.Response]:
        '''
            Send get request to OpenAlex, anticipating a corresponding JSON response for the selected endpoint
            
            :param endpoint -- The api endpoint for desired OpenAlex object 
            :param filter -- Optional filter parameter on the get request, by default not used
            :param search -- Optional parameter that will retrieve results that contain the parameter in the title, abstract or fulltext 
            :param group  -- Optional parameter that will group results by provided attributes
        '''
        with httpx.Client(transport=RetryTransport()) as client:
            parameters = {}
            if pagination:
                if items_per_page>200:
                    print(f'Maximum items per page is 200, less than the supplied {items_per_page} items per page. Setting maximum items per page to 200.')
                    items_per_page = 200
                parameters[QueryParams.items_per_page.value] = items_per_page

                if pagination_type is PaginationTypes.CURSOR:
                    if page:
                        raise Exception('Cursor pagination type is incompatible with the page parameter.')
                    parameters[QueryParams.cursor_pagination.value] = '*'
                
                if page:
                    if page > (max_page := (MAXIMUM_RESULTS_BASIC_PAGINATION//items_per_page)):
                        print(f'Maximum page value exceeds 10000 results limit. Setting maximum page value to suitable maximum: {max_page}.')
                        page = max_page
                    parameters[QueryParams.page.value] = page

            
            if select:
                select = ','.join(select)

            format_parameters = {
                QueryParams.select.value : select, 
                QueryParams.filter.value : filter,
                QueryParams.search.value : search,
                QueryParams.group.value : group,
                QueryParams.sort.value : sort
            }
            for type, option in format_parameters.items():
                if option:
                    parameters[type] = option                  

            first = send_request(client, 'GET', endpoint, parameters).json()         
            res = [first] if "meta" in first and "count" in first["meta"] and first["meta"]["count"] > 0 else []

            if res and pagination and pagination_type is PaginationTypes.CURSOR:
                # If a supplied number of pages, iterate through until count is reached
                def find_next_cursor(last_json):
                    if "meta" not in last_json and "next_cursor" not in last_json["meta"]:
                        raise Exception("Next cursor not found in cursor pagination response object")
                    
                    return last_json["meta"]["next_cursor"]
                
                next_cursor = find_next_cursor(res[-1])

                if pages_count:
                    for p in range(pages_count-1):
                        if not next_cursor:
                            raise Exception("No next cursor found for cursor pagination")    
                        content = update_cursor(client, next_cursor, endpoint, parameters).json()
                        next_cursor=find_next_cursor(res[-1])
                        if not(next_cursor and "results" in content and len(content["results"])):
                            raise Exception("Data emptied out before pages could be reached.")
                        
                        res.append(content)
                        print(f'Collected {p+2} pages.')
                # If not iterate until no next cursor is given.
                else:
                    response_count = 1
                    total_items = items_per_page
                    while next_cursor and (results := (content := update_cursor(client, next_cursor, endpoint, parameters).json()).get("results", None)) is not None and len(results):
                        res.append(content)
                        response_count+=1
                        total_items += len(content["results"])
                        next_cursor = find_next_cursor(content)
                        print(f'Collected {response_count} responses with a total of {total_items} items.')

                        # If the number of paginated responses has hit a set limit, write to disk and continue
                        if len(res) >= write_chunk_cutoff:
                            print(f'Chunk cutoff of {write_chunk_cutoff} reached. Writing to disk.')
                            WriteFx(res)
                            res.clear()
                    '''
                    Keep updating until content next_cursor is empty and results are empty
                    '''
            
            if res and WriteFx:
                WriteFx(res)
                
            return res