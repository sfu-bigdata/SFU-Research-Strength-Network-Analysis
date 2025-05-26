from io import StringIO
import polars as pl
import zstandard, glob
from pathlib import Path
from xopen import xopen
from typing import Optional
import json

def load_data(input_dir: Path, target_dir = None | str):
    dataframes: pl.DataFrame = {}

    if target_dir:
        child_directories = [Path(input_dir.joinpath(target_dir))]
    else:
        child_directories = [item for item in input_dir.iterdir() if item.is_dir()]

    for directory in child_directories:
        files = directory.glob('*.json.zst')
        directory_set = pl.DataFrame(nan_to_null=True)
        for file in files:
            with xopen(file, mode='rb') as f:
                new_dataframe = pl.read_json(f)
                
                # If this is part of a multiple page response
                if "results" in new_dataframe.columns:
                    new_dataframe = new_dataframe\
                        .select("results")\
                        .explode("results")\
                        .unnest("results")
                
                directory_set = pl.concat((directory_set, new_dataframe))
        
        if not directory_set.is_empty():
            dataframes[directory.name] = directory_set

    return dataframes

def preprocess():
    pass