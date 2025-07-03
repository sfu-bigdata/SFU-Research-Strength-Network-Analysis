import polars as pl
from neo4j import GraphDatabase, Result
from .conf import DatabaseConfig
import os
from ..utils.helpers import clear_directories, move_directories
from pathlib import Path
from .conf import ObjectNames
from .connect import N4J_Connection
from config import TableMap, NodeType
from .helpers import infer_node_types_from_file
from .relationships import Relationships

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
        clear_directories(output_directory, keepStructure=False)

    move_directories(input_directory, output_directory)

def load_nodes_into_db(connection: N4J_Connection, node_dir: Path):
    nodes = node_dir.glob('*.parquet')

    for node in nodes:
        stem = node.stem
        if stem not in TableMap:
            raise('Unsupported object type - can\'t infer from filename.')

        nodeType = (TableMap.get(stem))

        if nodeType not in ObjectNames:
            raise(f'GraphObjectType not implemented for node type: {nodeType}')
        
        graphObjectType = ObjectNames.get(nodeType)


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

        result = connection.execute_cypher_query(query)
        assert result._metadata.get('statuses')[0].get('status_description') == 'note: successful completion'
    
def load_relationships_into_db(connection: N4J_Connection, relationship_dir: Path):
    files = relationship_dir.glob('**/*.parquet')

    RelationshipsGen = Relationships()
    
    for file in files:
        start_node, end_node = infer_node_types_from_file(file)
        relationshipObj = RelationshipsGen.createRelationshipObject(start_node, end_node)

        query = f"""
        CALL apoc.periodic.iterate(
            "CALL apoc.load.parquet ('file:///relationships/{file.name}') YIELD value AS ROW",
            "
                WITH ROW,
                    ROW.`{relationshipObj.origin_id}` AS origin_id,
                    ROW.`{relationshipObj.target_id}` AS target_id,
                    apoc.map.clean(ROW, [\\"{relationshipObj.origin_id}\\", \\"{relationshipObj.target_id}\\"], []) as properties
                
                MATCH ({relationshipObj.origin_node.prefix}: {relationshipObj.origin_node.name} {{id: origin_id}})
                MATCH ({relationshipObj.target_node.prefix}: {relationshipObj.target_node.name} {{id: target_id}})
                MERGE ({relationshipObj.origin_node.prefix})-[r:{relationshipObj.rel_type}]->({relationshipObj.target_node.prefix})
                ON CREATE SET r = properties
                ON MATCH SET r += properties
            ",
            {{
                batchSize: 1000,
                parallel: false,
                retries: 5
            }}
        )
        """

        result = connection.execute_cypher_query(query)
        assert result._metadata.get('statuses')[0].get('status_description') == 'note: successful completion'

