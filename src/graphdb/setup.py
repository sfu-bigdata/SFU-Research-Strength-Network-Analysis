import polars as pl
from neo4j import GraphDatabase, Result
from .conf import DatabaseConfig
import os
from ..utils import helpers
from pathlib import Path
from ..processing.conf import TableMap, NodeType
from ..processing.graph import Relationships
from .conf import ObjectNames
from .connect import N4J_Connection

def db_setup(input_directory: Path, 
             output_directory: Path,
             clear_previous_contents: bool = False):
    '''
    Ensure that parquet files have been moved to the imports directory
    Throw an error if no files are found
    ''' 

    # Check to see if inputs has data
    if not input_directory.exists():
        raise ImportError(f'File input directory {input_directory} does not exist - can\'t migrate data')
    
    file_count = sum(len(files) for _, _, files in os.walk(input_directory))
    
    if not file_count:
        raise ImportError(f'No files found in the input directory: {input_directory}')
    
    # Check to see if something is in the target directory

    if output_directory.exists() and not clear_previous_contents and len(output_directory):
        raise ImportError(f'The target output directory is not empty {output_directory}')

    if output_directory.exists() and clear_previous_contents:
        helpers.clear_directories(output_directory, keepStructure=False)

    helpers.move_directories(input_directory, output_directory)

def load_parquet_into_db(connection: N4J_Connection, node_dir: Path, relationship_dir: Path):
    nodes = node_dir.glob('*.parquet')

    for node in nodes:
        stem = node.stem
        if stem not in TableMap:
            raise('Unsupported object type - can\'t infer from filename.')

        nodeType = (TableMap.get(stem))
        graphObjectType = ObjectNames.get(nodeType)

        print(f'name: {node.name}, graphObject: {graphObjectType}')

        query = f"""
        CALL apoc.periodic.iterate(
        "CALL apoc.load.parquet('file:///nodes/{stem}.parquet') YIELD value as row",
        "MERGE ({graphObjectType.prefix}:{graphObjectType.name} {{ id: row.id }})
        SET {graphObjectType.prefix} += row",
        {{
            batchSize: 1000,
            parallel: false,
            retries: 5
        }}
        )
        """

        print(query)
        
        result = connection.execute_cypher_query(query)

        if result._consumed or result.peek() is None:
            raise Exception(f'Failed to load parquet file for node: {node}')