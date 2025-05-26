'''
    Supply raw json files into data raw and convert to zst
'''
if __name__ == '__main__':
    import zstandard, json, io
    from pathlib import Path
    from os import path
    cur_dir = path.dirname(__file__)
    data_dir = Path(cur_dir).joinpath('data', 'raw')

    Compressor = zstandard.ZstdCompressor()
    directories = [entry for entry in data_dir.iterdir() if entry.is_dir()]

    for directory in directories:
        files = directory.glob('*.json')

        for file in files:
            with open(file, 'rb') as rf, open(file.with_suffix('.json.zst'), 'wb+') as wf:
                    with Compressor.stream_writer(wf) as stream_writer:
                         for chunk in iter(lambda: rf.read(4096), b''):
                            stream_writer.write(chunk)