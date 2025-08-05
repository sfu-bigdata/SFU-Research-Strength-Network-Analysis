from enum import Enum
from pathlib import Path
import re
from config import NodeType
# Enum class that will contain the values for helpful and reusable cypher queries
class CypherQueryCollection(Enum):
    CLEAR_SCHEMA = """
        CALL apoc.schema.assert({}, {}, true);
    """
    DROP_DATABASE = """
        DROP DATABASE IF EXISTS Neo4j;
    """
    DELETE_NODES = """
        CALL apoc.periodic.iterate(
        'MATCH (n) RETURN n', 'DETACH DELETE n', {batchSize:1000}
        )
        """
    SELECT_ALL_NODES = """
        MATCH (n) RETURN n
    """

def infer_node_type_from_file(file: Path) -> NodeType:
    '''
    For singular node type
    '''
    try:
        filename = file.stem.replace('__', '&&')
        dlm_idx = filename.find('_')
        stringType = filename[:dlm_idx].replace('&&', '_') if dlm_idx != -1 else file.stem.replace('__', '_')
        return (NodeType(stringType))
    except Exception:
        raise Exception("Unable to infer node types from file with filename: ", file.name)

def infer_node_types_from_file(file: Path) -> tuple[NodeType, NodeType]:

    try:
        filename = file.stem.replace('__', '&&').split('_')
        start, target = filename[0].replace('&&', '_'), filename[1].replace('&&', '_')
        return (NodeType(start), NodeType(target))
    except Exception:
        raise Exception('Unable to infer node types from file with filename: ', file.name)