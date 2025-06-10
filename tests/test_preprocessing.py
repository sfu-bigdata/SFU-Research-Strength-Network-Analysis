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

'''
def test_relationship_models_in_common():
    ldataframe = DataFrame({
        'apple': [1,2],
        'banana': [3,4],
        'cantaloupe':[5,6]
    })

    rdataframe = DataFrame({
        'apple': [2,3],
        'brocolli':[3,4],
        'carrot':[7,8],
        'cantaloupe':[5,6]
    })

    lcols, rcols = set(ldataframe.columns), set(rdataframe.columns)
    mutual_cols = list(lcols.intersection(rcols))

    # Test normally
    calculated = Graph.relationship(ldataframe, rdataframe, nodeTypes=(Graph.NodeType.author, Graph.NodeType.funder), in_common=True)
    
    expected = DataFrame(
        data={
            Graph.NodeType.author.value: mutual_cols,
            Graph.NodeType.funder.value: mutual_cols
        },
        schema=[Graph.NodeType.author.value, Graph.NodeType.funder.value]
    )

    assert(calculated.equals(expected))

    # When cols are not in the same order
    calculated = Graph.relationship(rdataframe, ldataframe, nodeTypes=(Graph.NodeType.funder, Graph.NodeType.author), in_common=True)
    assert(calculated.equals(expected))

    # When nothing is in common

    altdataframe = DataFrame({
        'zebra': [2,3,4],
        'maroon': [3,1,2]
    })
    calculated = Graph.relationship(altdataframe, rdataframe,
                                    nodeTypes=(Graph.NodeType.work, Graph.NodeType.institution),
                                    in_common=True)
    expected = DataFrame(data={}, schema=[Graph.NodeType.institution.value, Graph.NodeType.work.value])

    assert(calculated.equals(expected))
'''

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
    dataframe = DataFrame({
        'a':['b','c'],
        'd':['e','f'],
        'g':['h','i']
    })

    calculated = Graph.relationship(
        
    )