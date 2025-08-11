'''
extract.py
Extract the data from OpenAlex and save in compressed JSON format
'''

from .openalex_api import OpenAlexApi, APIEndpoints, PaginationTypes, institution_ids
import json, zstandard, io, csv, json
from pathlib import Path
from . import conf
from typing import Iterable, LiteralString

def convert_json_to_ndjson(data: list) -> io.StringIO:
    buffer = io.StringIO()

    for item in data:
        if isinstance(item, dict) and "results" in item:
            item = item["results"]
        for record in item:
            buffer.write(json.dumps(record)+'\n')

    return buffer

def convert_json_to_ndjson_chunked(data: list, chunks: int = 1024) -> Iterable[LiteralString]:
    chunked_col = []

    for item in data:
        if isinstance(item, dict) and "results" in item:
            item = item["results"]
        for record in item:
            chunked_col.append(json.dumps(record))
            if len(chunked_col) >= chunks:
                yield '\n'.join(chunked_col)+'\n'
                chunked_col.clear()

    if len(chunked_col):
        chunked_col.append('\n')
        yield '\n'.join(chunked_col)

# For usage as a functor
class WriteFunctor:
    def __init__(self, path : Path, filename: str, chunk_size: int = 1024 * 1024):
        self.path = path
        self.filename = filename
        self.extension = '.json.zst'
        self.suffix = 1
        self.chunk_size = chunk_size
    
    def __call__(self, data):
        # Write the provided data in compressed format
        #bytes = json.dumps(data).encode('utf-8')
        #compressed_data = zstandard.compress(bytes, level=10)
        self.path.mkdir(parents=True, exist_ok=True)
        filepath = self.path.joinpath(self.filename+'-'+str(self.suffix)+self.extension)
        print(f'Writing data to directory: {filepath}')
        
        '''
        # Uncompressed
        with open(self.path.joinpath(self.path, self.filename+'-'+str(self.suffix)+'_raw.json'), 'w') as f:
            json.dump(data,f)
        '''
        if not isinstance(data, list):
            data = [data]
        data = convert_json_to_ndjson_chunked(data)
        #jsonString = buffer.getvalue()

        try:
            compressor = zstandard.ZstdCompressor()
            with open(filepath, 'wb') as file:
                with compressor.stream_writer(file) as stream_writer:
                    for chunk in data:
                        bytes = chunk.encode('utf-8')
                        stream_writer.write(bytes)
                
                #file.write(compressed_data)
            self.suffix+=1
        except Exception as e:
            print(f'Unable to write to file: {filepath}\n{e}')
            filepath.unlink(missing_ok=True)

def extract(output_path: Path) -> None:
    '''
        Get all information relating to the U15 and SFU
        Store the results
    '''
    api = OpenAlexApi()
    for (institution, funder) in institution_ids:

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
            WriteFx = WriteFunctor(output_path.joinpath('works'), funder)
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
        print(f'Gathering all affiliated author objects for {institution}')
        filter = '%s:%s%s' % ('affiliations.institution.id', conf.BASE_URI, institution)
        api.retrieve_list(
            APIEndpoints.AUTHORS,
            pagination=True,
            pagination_type = PaginationTypes.CURSOR,
            filter=filter,
            WriteFx=WriteFunctor(output_path.joinpath('authors'), institution)
        )
    print('Finished collecting author data.')

    # Get all journal data
    print('Gathering information related to journals.')

    journal_path = conf.INPUTS_DIR.joinpath('api', 'journals.csv')
    with open(journal_path, 'r') as data:
        chunk_size = 50
        csv_reader = csv.reader(data)
        
        headers = next(csv_reader) # Title, ISSN
        
        issn_collection = []
        
        total_length = 0

        def get_data(data_collection, suffix):
            issn_collection = [issn for (_, issn) in data_collection]
            filter_query = '|'.join(issn_collection)
            filter = '%s:%s' % ('issn', filter_query)

            res_set = api.retrieve_list(
                APIEndpoints.SOURCES,
                pagination=True,
                pagination_type=PaginationTypes.CURSOR,
                filter=filter,
                WriteFx=WriteFunctor(output_path.joinpath('sources'), 'batch-'+suffix)
            )

            if res_set and issn_collection:

                for res in res_set:
                    results = res.get("results", {})
                    values = set(issn_collection)
                    
                    # Check to make sure requested journals there there
                    if results and values:
                        issns = []

                        for result in results:
                            issn = result.get("issn", None)
                            if issn is not None:
                                issns.append(issn)
                        issns = {issn for issnlist in issns for issn in issnlist}
                        
                        difference = {
                            name for (name, issn) in data_collection if issn not in issns 
                        }

                        if len(difference):
                            print('Some journals were unable to be retrieved: ')
                            print(difference, ' ')

                        nonlocal total_length
                        total_length+=len(results)
            data_collection.clear()

        for idx, (name, issn) in enumerate(csv_reader):
            issn_collection.append((name, issn))
            
            if (idx+1) % chunk_size == 0:
                get_data(issn_collection, str(idx+1))
        
        if issn_collection:
            get_data(issn_collection, '-final')
        print(f'Finished collection journal data with {total_length} journals.')

    print(f'Gathering funder information for institions as funders.')
    # Get all works funded by the institution or by its affiliated organizations
    funder_list = [funder for (_, funder) in institution_ids]
    filter = '%s:%s' % ('ids.openalex', '|'.join(funder_list))
    api.retrieve_list(
        APIEndpoints.FUNDERS,
        pagination=True,
        pagination_type = PaginationTypes.CURSOR,
        filter=filter,
        WriteFx = WriteFunctor(output_path.joinpath('funders'), funder)
    )
    print(f'Finished gathering funded works for funder institutions.')

    print(f'Gathering OpenAlex topic data objects.')
    api.retrieve_list(
        APIEndpoints.TOPICS,
        pagination=True,
        pagination_type=PaginationTypes.CURSOR,
        select=['id', 'display_name', 'field', 'subfield', 'domain'],
        WriteFx=WriteFunctor(output_path.joinpath('topics'), 'topics')
    )
    print(f'Finished gathering topics objects.')
    print('Data extraction complete.')
        
