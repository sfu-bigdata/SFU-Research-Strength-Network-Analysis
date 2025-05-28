import polars as pl
from pathlib import Path
from xopen import xopen
from typing import Optional

def load_data(input_dir: Path, target_dir : Optional[str] = None) -> dict[str, pl.DataFrame]:
    dataframes : dict[str, pl.DataFrame] = {}

    if target_dir:
        child_directories = [Path(input_dir.joinpath(target_dir))]
    else:
        child_directories = [item for item in input_dir.iterdir() if item.is_dir()]

    for directory in child_directories:
        files = directory.glob('**/*.json.zst')
        directory_set = None
        for file in files:
            with xopen(file, mode='rb') as f:
                new_dataframe = pl.read_json(f)
                
                # If this is part of a multiple page response
                if "results" in new_dataframe.columns:
                    new_dataframe = new_dataframe\
                        .select("results")\
                        .explode("results")\
                        .unnest("results")\
                        .drop('relevance_score')
                    
                directory_set = pl.concat((directory_set, new_dataframe), how='diagonal_relaxed') if directory_set is not None else new_dataframe
        
        if (directory_set is not None) and (not directory_set.is_empty()):
            dataframes[directory.name] = directory_set

    return dataframes


def clean_data(data: dict[str, pl.DataFrame]) -> dict[str, pl.DataFrame]:
    pass    

def save_as_parquet(data, output_path: Path):
    output_path.mkdir(parents=True, exist_ok=True)
    for k, v in data.items():
        v.write_parquet(Path.joinpath(output_path, k+'.parquet'))

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
    save_as_parquet(data, output_path)