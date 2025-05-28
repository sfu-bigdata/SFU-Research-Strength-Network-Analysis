import pytest
import processing.raw as ProcessingRaw
import processing.pruning_conf as Pruning
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

'''
@pytest.fixture
def dataset(input_path: Path) -> dict[str, DataFrame]:
    return ProcessingRaw.load_data(input_path)

#@pytest.mark.dependency(depends=['test_single_json', 'test_multiple_json'])
def test_pruning(dataset: dict[str, DataFrame]):
    for k, v in dataset.items():
        print(f'k: {k}, v: {v}')
        dataset[k] = Pruning.pruneData(k, v)
'''

@pytest.mark.dependency(depends=["test_single_json", "test_multiple_json"])
def test_save_as_parquet(input_path):
    OUTPUT_DIR = input_path.joinpath('..', 'output')
    data = ProcessingRaw.load_data(input_path)
    assert(len(data))
    ProcessingRaw.save_as_parquet(data, OUTPUT_DIR)