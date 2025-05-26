'''
extract.py
Extract the data from OpenAlex and save in compressed JSON format
'''

from .openalex_api import OpenAlexApi, APIEndpoints, PaginationTypes, institution_ids
import json, zstandard, io
from pathlib import Path
from . import conf

# For usage as a functor
class WriteFunctor:
    def __init__(self, path : Path, filename: str):
        self.path = path
        self.filename = filename
        self.extension = '.json.zst'
        self.suffix = 1

    def __call__(self, data):
        # Write the provided data in compressed format
        #bytes = json.dumps(data).encode('utf-8')
        #compressed_data = zstandard.compress(bytes, level=10)
        self.path.mkdir(parents=True, exist_ok=True)
        filepath = self.path.joinpath(self.filename+'-'+str(self.suffix)+self.extension)
        print(f'Writing data to directory: {filepath}')
        
        compressor = zstandard.ZstdCompressor()
        with open(filepath, 'ab+') as file:
            with compressor.stream_writer(file) as stream_writer:
                writer = io.TextIOWrapper(stream_writer, encoding='utf-8')
                json.dump(data, writer)
                writer.flush()
            
            #file.write(compressed_data)
        self.suffix+=1
        
def extract(output_path: Path) -> None:
    '''
        Get all information relating to the U15 and SFU
        Store the results
    '''
    api = OpenAlexApi()
    for (institution, funder) in institution_ids:
        
        # SFU does not have a publisher id, so this data could be redundant.
        '''
        # Get all published works where the institution is either the host or the parent of the host
        filter = '%s:%s%s' % ('primary_location.source.publisher_lineage', conf.BASE_URI, institution)
        publisher_works.append((api.retrieve_list(
                pagination=True,
                paginationTypes = PaginationTypes.CURSOR,
                filter=filter
        )).json())
        '''

        print(f'Gathering sources for institution: {institution}')
        # Get all hosted sources by institutions (may be better as SFU is not a publisher)
        filter = '%s:%s%s' % ('host_organization_lineage', conf.BASE_URI, institution)
        api.retrieve_list(
            APIEndpoints.SOURCES,
            pagination=True,
            pagination_type=PaginationTypes.CURSOR,
            filter=filter,
            WriteFx=WriteFunctor(output_path.joinpath('sources'), institution),
            write_chunk_cutoff=100
        )
        print(f'Finished gathering sources for institution: {institution}')

        print(f'Beginning retrieval of works for institution: {institution}')
        # Get all works where the institution has atleast one affiliated researcher involved
        filter = '%s:%s%s' % ('authorships.institutions.lineage', conf.BASE_URI, institution)
        api.retrieve_list(
            APIEndpoints.WORKS,
            pagination=True,
            pagination_type=PaginationTypes.CURSOR,
            filter=filter,
            WriteFx=WriteFunctor(output_path.joinpath('works'), institution),
            write_chunk_cutoff=100
        
        )
        print(f'Finished gathering affiliated works for institution: {institution}')


        print(f'Gathering funded works for funder: {funder}')
        # Get all works funded by the institution or by its affiliated organizations
        filter = '%s:%s%s' % ('grants.funder', conf.BASE_URI, funder)
        api.retrieve_list(
            APIEndpoints.WORKS,
            pagination=True,
            pagination_type = PaginationTypes.CURSOR,
            filter=filter,
            WriteFx = WriteFunctor(output_path.joinpath('funders'), funder)
        )
        print(f'Finished gathering funded works for funder: {funder}')

        

        # Get all affiliated institutions
        print(f'Gathering all affiliated institutions for: {institution}')
        filter = '%s:%s%s' % ('lineage', conf.BASE_URI, institution)
        api.retrieve_list(
            APIEndpoints.INSTITUTIONS,
            pagination=True,
            pagination_type = PaginationTypes.CURSOR,
            filter=filter,
            WriteFx= WriteFunctor(output_path.joinpath('institutions'), institution)
        )
        print(f'Finished gathering affiliated institutions for {institution}')


        #write_to_disk(publisher_works, path.join(output_path, 'publisher_works'))
        print('Finished writing data to disk')
        print(f'Completed gathering data for institution id: {institution}')

    # Get all authors that at some point claimed an affiliation with Simon Fraser University
    # SFU Institution ID
    print('Gathering SFU affiliated author objects.')
    (institution, _) = institution_ids[0]
    filter = '%s:%s%s' % ('affiliations.institution.lineage', conf.BASE_URI, institution)
    api.retrieve_list(
        APIEndpoints.AUTHORS,
        pagination=True,
        pagination_type = PaginationTypes.CURSOR,
        filter=filter,
        WriteFx=WriteFunctor(output_path.joinpath('authors'), institution)
    )
    print('Finished collecting author data.')