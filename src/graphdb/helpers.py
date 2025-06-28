from enum import Enum

# Enum class that will contain the values for helpful and reusable cypher queries
class CypherQueryCollection(Enum):
    DELETE_NODES = """
        CALL apoc.periodic.iterate(
        'MATCH (n) RETURN n', 'DETACH DELETE n', {batchSize:1000}
        )
        """
    SELECT_ALL_NODES = """
        MATCH (n) RETURN n
    """