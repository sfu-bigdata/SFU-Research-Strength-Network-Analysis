'''
test_integration.py
Tests for integration of ETL components.
Due to the time of retrieving sample data, these tests will assume data has already been extracted
'''
import processing.raw as ProcessingRaw
import src.api.conf as ApiConf
import pytest
import pathlib

'''
@pytest.fixture(scope="class")
def test_data():
    rawPath = pathlib.Path(__file__).parent.joinpath('data', 'raw')
    map = ProcessingRaw.load_data(rawPath)
    yield map

def test_load(test_data):
    directories = [entry for entry in ApiConf.OUTPUT_RAW_DATA_DIR.iterdir() if entry.is_dir()]
    
    for directory in directories:
        assert(directory.name in test_data)
        assert(test_data[directory.name] is not None and not test_data[directory.name].limit(1).collect().is_empty())

@pytest.fixture(scope="class")
def test_load_extracted_dataframes():
    map = ProcessingRaw.load_data(ApiConf.OUTPUT_RAW_DATA_DIR)    
    yield map

def test_extracted_load(test_load_extracted_dataframes):
    directories = [entry for entry in ApiConf.OUTPUT_RAW_DATA_DIR.iterdir() if entry.is_dir()]
    keys = test_load_extracted_dataframes.keys()

    for directory in directories:
        assert directory.name in keys
        assert not (test_load_extracted_dataframes[directory.name].limit(1).collect().is_empty())

'''
def test_preprocessing():
    INPUT_PATH = pathlib.Path(__file__).parent.joinpath('data', 'raw')
    OUTPUT_PATH = pathlib.Path(__file__).parent.joinpath('data', 'output')
    # Collect the data and save it as parquet
    ProcessingRaw.preprocess(INPUT_PATH, OUTPUT_PATH)
    pass