import pytest
import processing.raw as ProcessingRaw
import processing.pruning_conf as Pruning
import processing.graph as Graph
from pathlib import Path
from polars import DataFrame

@pytest.fixture
def input_path():
    from os import path
    
    CUR_DIR = path.dirname(__file__)
    DATA_DIR = Path(CUR_DIR).joinpath('data', 'raw')
    
    return DATA_DIR

@pytest.mark.dependency()
def test_single_json(input_path: Path):
    DIRECTORIES = [entry for entry in input_path.iterdir() if entry.is_dir()]
    for directory in DIRECTORIES:
        ProcessingRaw.load_data(directory, target_dir='single')
        
@pytest.mark.dependency()
def test_multiple_json(input_path: Path):
    DIRECTORIES = [entry for entry in input_path.iterdir() if entry.is_dir()]
    for directory in DIRECTORIES:
        ProcessingRaw.load_data(directory, target_dir='multiple')

@pytest.fixture
def dataset(input_path: Path) -> dict[str, DataFrame]:
    return ProcessingRaw.load_data(input_path)

@pytest.mark.dependency(depends=['test_single_json', 'test_multiple_json'])
def test_pruning(dataset: dict[str, DataFrame]):
    for k, v in dataset.items():
        dataset[k] = Pruning.PruningFunction(k).__call__(v)

@pytest.mark.dependency(depends=["test_single_json", "test_multiple_json", "test_pruning"])
def test_save_as_parquet(input_path):
    OUTPUT_DIR = input_path.joinpath('..', 'output')
    data = ProcessingRaw.load_data(input_path)
    assert(len(data))
    data = ProcessingRaw.clean_data(data)
    ProcessingRaw.save_as_parquet(data, OUTPUT_DIR)

def test_relationship_models():
    # Mock Dataframes
    ldataframe = DataFrame({
        'aqua': [1,2,3],
        'marine': ['a','b','c'],
        'emerald': [-1.0, 0., 1.0]
    })

    rdataframe = DataFrame({
        'ruby': [4,5,6],
        'sapphire': ['t','y','u'],
        'onyx': [1,2,3]
    })

    calculated = Graph.relationship(
        ldataframe, rdataframe,
        l_id = 'aqua',
        r_id = 'onyx',
        relationshipType= 'equals'
    )

    expected = DataFrame({
        ':END_ID': rdataframe.get_column('onyx').to_list(),
        ':START_ID': ldataframe.get_column('aqua').to_list(),
        ':TYPE': 'equals'
    })

    assert(calculated.equals(expected))
def test_node_models():
    # Mock Dataframe
    left = DataFrame({
        'a':['b','c'],
        'd':['e','f'],
        'g':['h','i']
    })

    right =  DataFrame({
        'x':['g','m'],
        'y':['j','f'],
        'z':['f','p']
    })

    relationships = Graph.Relationships()

    calculated = Graph.relationship(
        left,
        right,
        'a',
        'x',
        relationships.calculate_relationship(Graph.NodeType.work, Graph.NodeType.source)
    )

    expected = DataFrame({
        ':END_ID': right.get_column('x').to_list(),
        ':START_ID': left.get_column('a').to_list(),
        ':TYPE': relationships.RelationshipTypeMap[(Graph.NodeType.source, Graph.NodeType.work)]
    })

    assert calculated.equals(expected)

    calculated = Graph.relationship(
        left,
        right,
        'a',
        'x',
        relationships.calculate_relationship(Graph.NodeType.author, Graph.NodeType.work)
    )

    expected = DataFrame({
        ':END_ID': right.get_column('x').to_list(),
        ':START_ID': left.get_column('a').to_list(),
        ':TYPE': relationships.RelationshipTypeMap[(Graph.NodeType.author, Graph.NodeType.work)]
    })

    assert calculated.equals(expected)


    # Should throw an error 
    with pytest.raises(ValueError):
        calculated = Graph.relationship(
            left,
            right,
            'a',
            'x',
            relationships.calculate_relationship(Graph.NodeType.none, Graph.NodeType.none)
        )