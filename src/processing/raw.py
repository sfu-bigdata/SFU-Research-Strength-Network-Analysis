import polars as pl
from pathlib import Path
from typing import Optional
from .pruning_conf import PruningFunction, SecondaryInformation
from ..utils import helpers
from .conf import GraphTable,GraphDataCollection,GraphRelationship, designatedDirectories, schemas
from config import GEOGRAPHIC_DATA_LOCATION, NodeType

def preprocess_data_item(
    type: NodeType,
    data: pl.LazyFrame,
    output_path: Path
):
    print('Cleaning Data..')
    nodes = clean_data(type, data)
    print('Deriving secondary data from original dataset')
    node_path = output_path.joinpath('nodes')
    relationship_path = output_path.joinpath('relationships')
    generate_secondary_data(nodes, node_path, relationship_path)
    print(f'Saving data to output directory: {output_path}')
    print('Saving nodes...')
    save_graphtables_as_parquet([nodes], node_path)
    print('Finished writing to disk.')

def process_files(directory: Path, output_path: Path, single: bool = False):
    # Provide schema for certain columns that may be problematic
    try:
        schema = schemas[directory.name] if directory.name in schemas else None
        files = directory.glob('**/*.json.zst')
        for file in [file for file in files if file.is_file()]:
            lazyframe = pl.scan_ndjson(file, batch_size=1024, schema=schema, infer_schema_length=300, low_memory=True).lazy()
            preprocess_data_item(designatedDirectories[directory.name], 
                                 lazyframe,
                                 output_path.joinpath(file.name.split('.')[0], directory.name))

    except Exception as e:
        raise Exception(f'Unable to scan files for {file.name}\n{e}')
    

def process_data(input_dir: Path, output_dir: Path, target_dir : Optional[str] = None):

    if target_dir:
        child_directories = [Path(input_dir.joinpath(target_dir))]
    else:
        child_directories = [item for item in input_dir.iterdir() if item.is_dir()]


    for directory in child_directories:
        if directory.name not in designatedDirectories:
            raise Exception(f'Directory {directory.name} not found in designated directories. Update root config.')
        process_files(directory, output_dir)

def clean_data(nodetype: NodeType, data: pl.LazyFrame) -> GraphTable:
    pruned_data,type = PruningFunction(nodetype).__call__(data)
    return GraphTable(name=type.value, type=type, data=pruned_data)

def generate_secondary_data(table: GraphTable, node_path: Path, relationship_path: Path):
    secondaryInfo = SecondaryInformation()    
    
    print('Getting derived table information')
    derivedList = secondaryInfo.derive(table)

    print('Saving derived data to disk...')
    for data in derivedList:
        save_graphtables_as_parquet(data.nodes, node_path)
        save_relationships_as_parquet(data.relationships, relationship_path)


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

def save_as_parquet(data : pl.LazyFrame, output_path: Path):
    sfx = 0
    output_path = Path.joinpath(output_path.parent, (output_path.stem+'_'+str(sfx))+output_path.suffix)
    while output_path.exists():
        sfx+=1
        splitText = output_path.stem.split('_')
        splitText[-1] = str(sfx)
        output_path = Path.joinpath(output_path.parent, ('_'.join(splitText))+output_path.suffix)
        print(f'File with name already exists. Trying again with: {output_path}')

    print(f'Saving data to: {output_path}')
    data.sink_parquet(
        path=output_path,
        maintain_order=False,
        row_group_size=100,
        compression='zstd'
    )

def save_graphtables_as_parquet(data: list[GraphTable], output_path: Path):
    output_path.mkdir(parents=True, exist_ok=True)
    
    for table in data:
        target_path = Path.joinpath(output_path, table.name.replace('_', '__')+'.parquet')
        print(f'Saving node data...')
        save_as_parquet(table.data, target_path)

    return

def save_relationships_as_parquet(data: list[GraphRelationship], output_path: Path):
    output_path.mkdir(parents=True, exist_ok=True)
    for table in data:
        target_path = Path.joinpath(output_path, table.start_type.value.replace('_', '__')+'_'+table.target_type.value.replace('_', '__')+'_relationship.parquet')
        print(f'Saving relationship data...')
        save_as_parquet(table.data, target_path)
        
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
    output_path = output_path.joinpath('nodes')

    print(f"Saving geographic data to: {output_path}")
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
    process_data(input_dir, output_path, optional_target_dir)
    print('Adding additional data...')
    print('Processing geographic information')
    process_geographic_data(GEOGRAPHIC_DATA_LOCATION, output_path.joinpath('geographic_data', NodeType.geographic.value))
    print('Finished generating Parquet.')