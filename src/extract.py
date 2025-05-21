'''
extract.py
Extract the data from OpenAlex and save in compressed JSON format
'''

import polars as pl
from openalex_api import OpenAlexApi, APIEndpoints, PaginationTypes, institution_ids
from config import conf
import json, zstandard, os.path as path

def write_to_disk(data, path):
    bytes = json.dumps(data).encode('utf-8')
    compressed_data = zstandard.compress(bytes, level=10)

    with open(path , 'wb') as file:
        file.write(compressed_data)

def extract():
    '''
        Get all information relating to the U15 and SFU
        Store the results
    '''
    api = OpenAlexApi()

    institution_works, funded_works, institutions = [], [], []
    for institution in institution_ids:

        '''
        # Get all published works where the institution is either the host or the parent of the host
        filter = '%s:%s%s' % ('primary_location.source.publisher_lineage', conf.BASE_URI, institution)
        publisher_works.append((api.retrieve_list(
                pagination=True,
                paginationTypes = PaginationTypes.CURSOR,
                filter=filter
        )).json())
        '''

        # Get all works where the institution has atleast one affiliated researcher involved
        filter = '%s:%s%s' % ('authorships.institutions.lineage', conf.BASE_URI, institution)
        institution_works.append((api.retrieve_list(
            pagination=True,
            PaginationTypes=PaginationTypes.CURSOR,
            filter=filter
        )).json())

        # Get all works funded by the institution or by its affiliated organizations
        filter = '%s:%s%s' % ('grants.funder', conf.BASE_URI, institution)
        funded_works.append((api.retrieve_list(
            pagination=True,
            PaginationTypes = PaginationTypes.CURSOR,
            filter=filter
        )).json())

        # Get all affiliated institutions
        filter = '%s:%s%s' % ('lineage', conf.BASE_URI, institution)
        institutions.append((api.retrieve_list(
            pagination=True,
            PaginationTypes = PaginationTypes.CURSOR,
            filter=filter
        )).json())


    
    # Write all data to disk in compressed format
    output_path = path.join(path.curdir, '..', conf.OUTPUT_RAW_DATA_DIR)
    write_to_disk(institution_works, path.join(output_path, 'works'))
    #write_to_disk(publisher_works, path.join(output_path, 'publisher_works'))
    write_to_disk(funded_works, path.join(output_path, 'funders'))
    write_to_disk(institutions, path.join(output_path, 'institutions'))

    # Get all authors that at some point claimed an affiliation with Simon Fraser University
    authors = []
    filter = '%s:%s%s' % ('affiliations.institution.lineage', conf.BASE_URI, institution)
    authors.append((api.retrieve_list(
        pagination=True,
        PaginationTypes = PaginationTypes.CURSOR,
        filter=filter
    )).json())

    write_to_disk(authors, path.join(output_path, 'authors'))

