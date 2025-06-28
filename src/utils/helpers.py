'''
helpers.py
Contains helpers functions for usage throughout application, including setup
'''
from typing import Optional
import pathlib, shutil, os

# Clear the data output repository for extracting new data
def removeNested(path: pathlib.Path):
    if path.is_dir():
        for dir in path.iterdir():
            removeNested(dir)
    elif path.is_file() or path.is_symlink():
        path.unlink()

def clear_directories(
        target: pathlib.Path,
        keepStructure: bool = False
) -> None:
    '''
    Utility function for removal of temporary data outputs.
    @param targetDirs - Optional list of target child directores to delete. Will remove otherwise.
    @param keepStructure - Whether or not to maintain the directory structure. Will remove the directory otherwise.
    '''
    if target.exists():
        removal_fx = shutil.rmtree if not keepStructure else removeNested
        removal_fx(target)
        print('Cleared directory')
    else:
        print('Target directory does not exist.')
    
    return

def move_directories(input_dir : pathlib.Path, output_dir : pathlib.Path):
    try:
        shutil.move(input_dir, output_dir)
    except Exception as e:
        print(f'ERROR: {e}')
    
    print(f'Moved directory data: {input_dir} to {output_dir}')