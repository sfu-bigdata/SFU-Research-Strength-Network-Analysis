import pytest
import processing.raw as ProcessingRaw
from pathlib import Path

@pytest.fixture
def input_path():
    from os import path
    
    CUR_DIR = path.dirname(__file__)
    DATA_DIR = Path(CUR_DIR).joinpath('data', 'raw')
    
    return DATA_DIR


def test_single_json(input_path: Path):
    DIRECTORIES = [entry for entry in input_path.iterdir() if entry.is_dir()]
    for directory in DIRECTORIES:
        ProcessingRaw.load_data(directory, target_dir='single')

def test_multiple_json(input_path: Path):
    DIRECTORIES = [entry for entry in input_path.iterdir() if entry.is_dir()]
    for directory in DIRECTORIES:
        ProcessingRaw.load_data(directory, target_dir='multiple')