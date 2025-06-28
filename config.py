from pathlib import Path
from os import environ

BASE_DIR = Path(__file__).parent
DATABASE_OUTPUT_DIR = BASE_DIR.joinpath(environ.get('DATABASE_IMPORT_DIR', 'database/db/imports'))
DATABASE_TEST_DATA_INPUT_DIR = BASE_DIR.joinpath('tests','data', 'output')