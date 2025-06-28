'''
test_database_init.py
Collection of tests concerning the initialization of a Neo4j Database

These tests require a running connectable database instance.
'''
import src.graphdb.helpers as GraphHelpers
from src.graphdb.connect import N4J_Connection, ConnectionType
from src.graphdb.conf import DatabaseConfig, ObjectNames, GraphObject, GraphType
import pytest
from src.graphdb.setup import db_setup, load_parquet_into_db
import os
from config import DATABASE_OUTPUT_DIR
from src.processing.graph import Relationships

@pytest.fixture(scope="session")
def db_fx_setup():
    # Check to see if the input directory is empty or not
    file_count = sum(len(files) for _, _, files in os.walk(DATABASE_OUTPUT_DIR))
    
    if not file_count:
        try:
            db_setup()
        except ImportError as e:
            print(f'Error: {e}')

'''
def test_http_connection():
    connection = N4J_Connection(
        targetAddress=DatabaseConfig.targetAddress,
        port = DatabaseConfig.httpClient,
        connectionType=ConnectionType.neo4j
    )

    connection.connect()
    connection.close()
'''
'''
@pytest.mark.dependency()
def test_bolt_connection():

    # Should attempt to connect on initialization
    connection = N4J_Connection(
        targetAddress=DatabaseConfig.targetAddress,
        port = DatabaseConfig.boltClient,
        connectionType=ConnectionType.bolt
    )

    # Close the connection
    connection.close()
'''

'''
From here on out only execute the tests if the ones above were successful
'''

'''
'''
@pytest.fixture(scope="class")
def connection():
    connection = N4J_Connection(
        targetAddress=DatabaseConfig.targetAddress,
        port=DatabaseConfig.boltClient,
        connectionType=ConnectionType.bolt
    )
    return connection

'''
@pytest.mark.dependency(depends=['test_bolt_connection'])
def test_initialization(connection):
    connection.execute_cypher_query(GraphHelpers.CypherQueryCollection.SELECT_ALL_NODES.value)
'''
'''
#@pytest.mark.dependency(depends=['test_bolt_connection'])
def test_constraints(connection, db_fx_setup):
    for _, graphObject in ObjectNames.items():
        connection.create_node_constraints(graphObject.name, graphObject.prefix, {
            "id": "IS UNIQUE"
        })

def test_create_node_indexes(connection, db_fx_setup):
    for _, GraphObject in ObjectNames.items():
        connection.create_indexes(GraphObject.name, GraphObject.prefix, set(["id"]), GraphType.NODE)

def test_create_relationship_constraints(connection, db_fx_setup):
    relationships = Relationships()
    relationshipTypes = relationships.RelationshipTypeMap

    for _, type in relationshipTypes.items():
        connection.create_relationship_constraints(
            type,
            set(['id']),
            'IS UNIQUE'
        )
'''

def test_load_db(connection, db_fx_setup):
    load_parquet_into_db(connection, DATABASE_OUTPUT_DIR.joinpath('nodes'), DATABASE_OUTPUT_DIR.joinpath('relationships'))