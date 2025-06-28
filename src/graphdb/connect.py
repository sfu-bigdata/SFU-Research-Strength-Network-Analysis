'''
connect.py
Connect to remote Neo4j database and perform actions upon it
'''
from enum import Enum
from typing import Optional
from neo4j import GraphDatabase, Result
from .conf import GraphType, GraphObject

class ConnectionType(Enum):
    bolt = 'bolt://'
    neo4j = 'neo4j://'

class N4J_Connection(object):

    def _connect(self):
        try:
            self._driver = GraphDatabase.driver(self.combinedAddress, auth=(self.username, self.password)) if self.authentication else GraphDatabase.driver(self.combinedAddress)
            self._driver.verify_connectivity()
            print(f'Connected to database at address: {self.combinedAddress}')
        except Exception as e:
            print(f'Failed to connected to database with error: {e}')
            self._driver = None
            raise e

    def close(self):
        if self._driver:
            self._driver.close()
            self._driver = None
            print('Shut down database connection.')
    
    def __init__(
        self,
        targetAddress : str,
        port: str,
        connectionType: ConnectionType,
        authentication: bool=False,
        username: Optional[str]=None,
        password: Optional[str]=None,
        database: str = 'neo4j'
    ):
        self.authentication = authentication
        self.username = username
        self.password = password
        self.targetAddress = targetAddress
        self.port = port
        self.connectionType = connectionType
        self.combinedAddress = '%s%s:%s' % (self.connectionType.value, self.targetAddress, self.port)
        # Community edition only allows for a single database
        self.database = database
        self._driver = None

        self._connect()

    def __del__(self):
        self.close()
    
    '''
    Doesn't work on community - only enterprise
    def _create_if_not_exists(self, database: str):
        query = f"""
            CREATE DATABASE {database} IF NOT EXISTS
        """
        with self._driver.session(database="system") as session:
            try:
                session.run(query)
                print(f"Database available at ${database}")
            except Exception as e:
                print(f"Error creating database: {e}")
    '''

    def create_node_constraints(self, fullName: str, pfx: str, constraints : dict[str, str]):
        # Iterate throught the constraints and apply them
        with self._driver.session(database=self.database) as session:
            for field, condition in constraints.items():
                query = f"""
                CREATE CONSTRAINT IF NOT EXISTS FOR ({pfx}:{fullName}) REQUIRE {pfx}.{field} {condition};
                """
                session.run(query)
                print(f"Executed constraint: {condition} on :{fullName}({field})")


    def create_relationship_constraints(self, 
                                        relationship_type: str,
                                        fields: set[str], 
                                        condition: str):
        with self._driver.session(database=self.database) as session:
            constraint_name = '_'.join([relationship_type] + list(fields) + [condition.lower().replace(' ', '_')])
            joined_fields = ', '.join(['r.'+f for f in fields])
            query = f"""
                CREATE CONSTRAINT {constraint_name} IF NOT EXISTS
                FOR ()-[r:{relationship_type}]-() REQUIRE ({joined_fields}) {condition}
            """
            session.run(query)
            print(f'Executed constraint on [r: {relationship_type}] with: REQUIRE {joined_fields} {condition}')

    def create_indexes(self, 
                       fullName: str, 
                       pfx: str, 
                       fields : set[str],
                       type : GraphType
                       ):
        with self._driver.session(database=self.database) as session:
            if type == GraphType.NODE:
                prefixed = [(pfx+'.'+field) for field in fields]
                combined = ', '.join(prefixed)
                cmd = f"""
                CREATE INDEX IF NOT EXISTS FOR ({pfx}:{fullName}) ON ({combined});
                """
                session.run(cmd)

                print(f'Created index on ({pfx}.{fullName}) for field: ({combined})')

            elif type==GraphType.RELATIONSHIP:
                raise(f'Not yet implemented')
            
            else:
                raise(f'Error unsupported node type: {type}')

    def execute_cypher_query(self, query, parameters=None) -> Result:
        try:
            with self._driver.session(database=self.database) as session:
                res = session.run(query)
                print(f"Executed query: {query}")
                return res
        except Exception as e:
            print(f"Failed to execute the query: {query}\n{e}")
            