import polars as pl
from pathlib import Path
from typing import Optional
from .pruning_conf import PruningFunction
from utils import helpers

def load_data(input_dir: Path, target_dir : Optional[str] = None) -> dict[str, pl.LazyFrame]:
    dataframes : dict[str, pl.LazyFrame] = {}

    if target_dir:
        child_directories = [Path(input_dir.joinpath(target_dir))]
    else:
        child_directories = [item for item in input_dir.iterdir() if item.is_dir()]

    def process_files(directory: Path, single: bool = False):
        try:
            path = directory.joinpath('**/*.json.zst') if not single else directory.joinpath('*/*.json.zst')
            new_dataframe = pl.scan_ndjson(path)
            return new_dataframe

        except Exception as e:
            print(f'Unable to scan files for {directory}')

    for directory in child_directories:
        dataframes[directory.name] = process_files(directory)

    return dataframes

def clean_data(data: dict[str, pl.LazyFrame]) -> dict[str, pl.LazyFrame]:
    for k, v in data.items():
        data[k] = PruningFunction(k).__call__(v)
    return data

def save_as_parquet(data, output_path: Path):
    output_path.mkdir(parents=True, exist_ok=True)
    for k, v in data.items():
        v.write_parquet(Path.joinpath(output_path, k+'.parquet'))

def save_lazyframe_as_parquet(data: dict[str, pl.LazyFrame], output_path: Path):
    if output_path.exists():
        helpers.clear_directories(output_path, keepStructure=True)

    output_path.mkdir(parents=True, exist_ok=True)

    for k, v in data.items():
        print(f'Saving {k} as parquet.')
        v.sink_parquet(Path.joinpath(output_path, k+'.parquet'),
                       maintain_order=False,
                       row_group_size=1000,
                       compression='zstd')

def preprocess(
        input_dir: Path,
        output_path: Path,
        optional_target_dir: Optional[str] = None
):
    '''
    Convenience function that will just to run the processing, cleaning and saving of data in one go.
    '''
    print('Loading Data...')
    data = load_data(input_dir, optional_target_dir)
    print('Cleaning Data..')
    data = clean_data(data)
    print(f'Saving data to output directory: {output_path}')
    save_lazyframe_as_parquet(data, output_path)