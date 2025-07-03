from enum import Enum
from pathlib import Path
import re
from config import NodeType
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

def infer_node_types_from_file(file: Path) -> tuple[NodeType, NodeType]:

    try:
        filename = file.stem.replace('__', '&&').split('_')
        start, target = filename[0].replace('&&', '_'), filename[1].replace('&&', '_')
    except Exception:
        raise Exception('Unable to infer node types from file with filename: ', file.name)

    return (NodeType(start), NodeType(target))


