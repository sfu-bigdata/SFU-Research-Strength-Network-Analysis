import polars as pl
from neo4j import GraphDatabase, Result
from .conf import DatabaseConfig, GraphObject
import os
from ..utils.helpers import clear_directories, move_directories
from pathlib import Path
from .conf import ObjectNames, GraphType
from .connect import N4J_Connection
from config import TableMap, NodeType, DATABASE_OUTPUT_DIR
from .helpers import infer_node_types_from_file, infer_node_type_from_file, CypherQueryCollection
from .relationships import Relationships, RelationshipObject, PropertyType, PropertyRelationship
from typing import Optional

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

def load_into_db(connection: N4J_Connection,
                 input_dir: Path,
                 load_nodes = True,
                 load_relationships = True,
                 propertyRelationships : list[PropertyRelationship] = []):
    '''
    Load into the database using folder structure to infer the type
    Assumes the structure:
    Top-Level
        NodeType
            DataType - (nodes, relationships)
                Data
    '''

    if not (load_nodes or load_relationships):
        return

    top_level = [entry for entry in input_dir.iterdir() if entry.is_dir()]
    
    for tl in top_level:
        types = [type for type in tl.iterdir() if tl.is_dir()]

        if load_nodes:
            for datatype in types:
                load_nodes_into_db(connection=connection,
                                    node_dir=datatype.joinpath('nodes'))

    top_level = [entry for entry in input_dir.iterdir() if entry.is_dir()]
    
    for tl in top_level:
        types = [type for type in tl.iterdir() if tl.is_dir()]

        if load_relationships:
            for datatype in types:
                load_relationships_into_db(connection=connection,
    
                                            relationship_dir=datatype.joinpath('relationships'))
            
            for prel in propertyRelationships:
                load_relationship_property_based(connection, prel.relationship, prel.properties, prel.propertyType)


def load_nodes_into_db(connection: N4J_Connection, 
                       node_dir: Path
                       ):
    
    nodes = node_dir.glob('**/*.parquet')

    for node in nodes:
        nodeType = infer_node_type_from_file(node)
        if nodeType not in ObjectNames:
            raise(f'GraphObjectType not implemented for node type: {nodeType}')

        graphObjectType = ObjectNames.get(nodeType)
        path = node.relative_to(DATABASE_OUTPUT_DIR).as_posix()
        query = f"""
        CALL apoc.periodic.iterate(
        "CALL apoc.load.parquet('file:///{path}') YIELD value as row",
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

def load_relationship_property_based(connection: N4J_Connection, 
                                    relationship: RelationshipObject,
                                    properties: dict[str, str],
                                    propertyType: PropertyType):
    
    origin, target = relationship.origin_node, relationship.target_node
    origin_node_prefix = origin.prefix+'_ORIGIN_NODE'
    target_node_prefix = target.prefix+'_TARGET_NODE'

    if propertyType == PropertyType.ONE_TO_ONE:
        query = f"""
            CALL apoc.periodic.iterate(
                "MATCH ({target_node_prefix}: {target.name}) RETURN {target_node_prefix}",
                "MATCH ({origin_node_prefix}: {origin.name} {properties})
                MERGE ({origin_node_prefix})-[r:{relationship.rel_type}]->({target_node_prefix})", 
                {{
                    batchSize: 1000,
                    parallel: false,
                    retries: 5
                }}       
            )
        """
    elif propertyType == PropertyType.CONTAINED:
        match = " AND ".join([origin_node_prefix+'.`'+key+'`' + " IN " +target_node_prefix+'.`'+value+'`' for key, value in properties.items()])
        query = f"""
        CALL apoc.periodic.iterate(
            "MATCH ({target_node_prefix}: {target.name}) RETURN {target_node_prefix}",
            "MATCH ({origin_node_prefix}: {origin.name})
            WHERE {match}
            MERGE ({origin_node_prefix})-[r:{relationship.rel_type}]->({target_node_prefix})", 
            {{
                batchSize: 1000,
                parallel: false,
                retries: 5
            }}       
        )
        """
    else:
        raise Exception('Property Type not implemented for: ', propertyType)

    result = connection.execute_cypher_query(query)
    assert result._metadata.get('statuses')[0].get('status_description') == 'note: successful completion'


def load_relationships_into_db(connection: N4J_Connection, relationship_dir: Path):
    files = relationship_dir.glob('**/*.parquet')

    RelationshipsGen = Relationships()
    
    for file in files:
        start_node, end_node = infer_node_types_from_file(file)
        relationshipObj = RelationshipsGen.createRelationshipObject(start_node, end_node)

        path = file.relative_to(DATABASE_OUTPUT_DIR).as_posix()

        origin_node_prefix = relationshipObj.origin_node.prefix+'_ORIGIN_NODE'
        target_node_prefix = relationshipObj.target_node.prefix+'_TARGET_NODE'

        query = f"""
        CALL apoc.periodic.iterate(
            "CALL apoc.load.parquet ('file:///{path}') YIELD value AS ROW",
            "
                WITH ROW,
                    ROW.`{relationshipObj.origin_id}` AS origin_id,
                    ROW.`{relationshipObj.target_id}` AS target_id,
                    apoc.map.clean(ROW, [\\"{relationshipObj.origin_id}\\", \\"{relationshipObj.target_id}\\"], []) as properties
                
                MATCH ({origin_node_prefix}: {relationshipObj.origin_node.name} {{id: origin_id}})
                MATCH ({target_node_prefix}: {relationshipObj.target_node.name} {{id: target_id}})
                MERGE ({origin_node_prefix})-[r:{relationshipObj.rel_type}]->({target_node_prefix})
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

def setup_full(connection: N4J_Connection,
                clear_previous_contents: bool,
                node_constraints: Optional[dict[GraphObject, dict[str, str]]] = None,
                relationship_constraints: Optional[dict[tuple[GraphObject, GraphObject], dict[set[str], str]]] = None,
                indexes: Optional[dict[GraphObject, set[str]]]= None
               ):

    if clear_previous_contents:
        connection.execute_cypher_query(CypherQueryCollection.DELETE_NODES.value)
        connection.execute_cypher_query(CypherQueryCollection.CLEAR_SCHEMA.value)

    # Define constraints
    if node_constraints is not None:
        for graphObject, constraints in node_constraints.items():
            connection.create_node_constraints(graphObject.name, graphObject.prefix, constraints)
    else:
        for _, graphObject in ObjectNames.items():
            connection.create_node_constraints(graphObject.name,  graphObject.prefix, {
                "id": "IS UNIQUE"
            })
    
    if relationship_constraints is not None:
        for relationship, (fields, constraints) in relationship_constraints.items():
            connection.create_relationship_constraints(
                relationship,
                fields,
                constraints
            )
    
    #define indexes
    if indexes is not None:
        for GraphObject, fields in indexes.items():
            connection.create_indexes(
                GraphObject.name,
                GraphObject.prefix,
                fields,
                GraphType.NODE
            )
    else:
        for _, GraphObject in ObjectNames.items():
            connection.create_indexes(GraphObject.name, GraphObject.prefix, set(['id']), GraphType.NODE)

    # Load nodes into DB
    load_into_db(connection=connection, input_dir=DATABASE_OUTPUT_DIR)