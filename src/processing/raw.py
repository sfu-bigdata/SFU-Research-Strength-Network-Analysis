import polars as pl
from pathlib import Path
from typing import Optional
from .graph import generateGraphNodes
from .pruning_conf import PruningFunction, SecondaryInformation
from ..utils import helpers
from .conf import GraphTable,GraphDataCollection,GraphRelationship, designatedDirectories
from config import GEOGRAPHIC_DATA_LOCATION



def load_data(input_dir: Path, target_dir : Optional[str] = None) -> dict[str, pl.LazyFrame]:
    dataframes : dict[str, pl.LazyFrame] = {}

    if target_dir:
        child_directories = [Path(input_dir.joinpath(target_dir))]
    else:
        child_directories = [item for item in input_dir.iterdir() if item.is_dir()]

    def process_files(directory: Path, single: bool = False):
        try:
            path = directory.joinpath('**/*.json.zst') if not single else directory.joinpath('*/*.json.zst')
            new_dataframe = pl.scan_ndjson(path, batch_size=1024, low_memory=True)
            return new_dataframe

        except Exception as e:
            print(f'Unable to scan files for {directory}')
    
    for directory in child_directories:
        if directory.name not in designatedDirectories:
            raise(f'Directory {directory.name} not found in designated directories. Update root config.')
        dataframes[designatedDirectories[directory.name]] = process_files(directory)

    return dataframes

def clean_data(data: dict[str, pl.LazyFrame]) -> list[GraphTable]:
    dataset = []
    for k, v in data.items():
        pruned_data,type = PruningFunction(k).__call__(v)
        dataset.append(GraphTable(name=type.value, type=type, data=pruned_data))

    return dataset

def generate_secondary_data(GraphTables: list[GraphTable], node_path: Path, relationship_path: Path):
    secondaryInfo = SecondaryInformation()    
    for table in GraphTables:
        print('Getting derived table information')
        derivedList = secondaryInfo.derive(table)
        print('Saving derived data to disk...')
        
        for data in derivedList:
            save_graphtables_as_parquet(data.nodes, node_path)
            save_relationships_as_parquet(data.relationships, relationship_path)

def save_as_parquet(data, output_path: Path):
    output_path.mkdir(parents=True, exist_ok=True)
    for k, v in data.items():
        v.write_parquet(Path.joinpath(output_path, k+'.parquet'))

def save_lazyframe_as_parquet(data: dict[str, pl.LazyFrame], output_path: Path):
    if output_path.exists():
        helpers.clear_directories(output_path, keepStructure=True)

    output_path.mkdir(parents=True, exist_ok=True)

    for k, v in data.items():
        print(f'Saving {k} as parquet.')
        v.sink_parquet(Path.joinpath(output_path, k+'.parquet'),
                       maintain_order=False,
                       row_group_size=1000,
                       compression='zstd')

def save_graphtables_as_parquet(data: list[GraphTable], output_path: Path):
    output_path.mkdir(parents=True, exist_ok=True)
    
    for table in data:
        target_path = Path.joinpath(output_path, table.name+'.parquet')
        print(f'Saving node data to: {target_path}')
        table.data.sink_parquet(
            path=target_path,
            maintain_order=False,
            row_group_size=100,
            compression='zstd'
        )
        
    return

def save_relationships_as_parquet(data: list[GraphRelationship], output_path: Path):
    output_path.mkdir(parents=True, exist_ok=True)
    
    for table in data:
        target_path = Path.joinpath(output_path, table.start_type.value+'_'+table.target_type.value+'_relationship.parquet')
        
        print(f'Saving relationship to: {target_path}')
        table.data.sink_parquet(
            path=target_path,
            maintain_order=False,
            row_group_size=100,
            compression='zstd'
        )
        
    return

def process_geographic_data(input_path: Path, output_path: Path):
    if not input_path.exists():
        raise(f'Geographic data location not found at location: {input_path}')
    
    geodata = pl.read_json(input_path)
    geodata = geodata.unpivot(
        on=geodata.columns,
        variable_name="continent",
        value_name="country"
    )\
        .explode('country')\
        .unnest('country')
    
    geodata = geodata.rename({'name':'country_name', 'country_code': 'id'}).unique(keep='first', subset=['id'])

    output_path.mkdir(parents=True, exist_ok=True)
    geodata.write_parquet(file=output_path.joinpath('geographic.parquet') ,compression='zstd')

def preprocess(
        input_dir: Path,
        output_path: Path,
        optional_target_dir: Optional[str] = None
):
    '''
    Convenience function that will just to run the processing, cleaning and saving of data in one go.
    '''
    # Clear the previous parquet
    helpers.clear_directories(output_path)
    print('Loading Data...')
    data = load_data(input_dir, optional_target_dir)
    print('Cleaning Data..')
    graphTables = clean_data(data)
    print('Deriving secondary data from original dataset')
    node_path = output_path.joinpath('nodes')
    relationship_path = output_path.joinpath('relationships')
    generate_secondary_data(graphTables, node_path, relationship_path)
    print('Generating relationship tables and node tables.')
    nodes, relationships = generateGraphNodes(graphTables)
    print(f'Saving data to output directory: {output_path}')
    print('Saving nodes...')
    save_graphtables_as_parquet(nodes, node_path)
    print('Saving relationships...')
    save_graphtables_as_parquet(relationships, relationship_path)
    print('Finished writing to disk.')

    print('Adding additional data...')
    print('Processing geographic information')
    process_geographic_data(GEOGRAPHIC_DATA_LOCATION, node_path)