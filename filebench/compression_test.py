import pyarrow.parquet as parquet
import pyarrow.feather as feather
import pyarrow.orc as orc
import pyarrow.csv as csv
import pyarrow as pa
from filebench.utils.measurements import memory_func, time_func
from pathlib import Path
from itertools import product
from filebench.models import Test
import datetime
from filebench.constants import PATHS, FILETYPES, PATH_DICT, COMPRESSIONS, ORIGINAL_CSV_FILE
import dataclasses
import pandas as pd
from filebench.utils import get_suffixes
from tqdm import tqdm


def write_and_compress_file(df, filepath, filetype, compression):
    if filetype == 'csv':
        table = pa.Table.from_pandas(df)
        if compression in ['bz2', 'brotli', 'gzip', 'lz4', 'zstd']:
            with pa.CompressedOutputStream(filepath, compression) as out:
                csv.write_csv(table, out)
        else:
            csv.write_csv(table, filepath)
    elif filetype == 'parquet':
        table = pa.Table.from_pandas(df)
        parquet.write_table(table, filepath, compression=compression)
    elif filetype == 'orc':
        table = pa.Table.from_pandas(df)
        orc.write_table(table, filepath, compression=compression)
    elif filetype == 'feather':
        feather.write_feather(df, filepath, compression=compression)


def load_data(filepath, filetype, compression='uncompressed'):
    """Read entire file convert df and get shape"""

    if filetype == 'parquet':
        df = parquet.read_table(filepath).to_pandas()
    elif filetype == 'orc':
        df = orc.read_table(filepath).to_pandas()
    elif filetype == 'feather':
        df = feather.read_feather(filepath)
    elif filetype == 'csv':
        if compression in ['bz2', 'brotli', 'gzip', 'lz4', 'zstd']:
            with pa.CompressedInputStream(pa.OSFile(str(filepath)), compression) as input:
                table = csv.read_csv(input)
        else:
            table = csv.read_csv(filepath)
        
        df = table.to_pandas()

    return df.shape


def compression_test(df, filepaths, compression_dict, repeat=1):
    test_start = datetime.datetime.now(datetime.timezone.utc).isoformat()

    results = []
    for test_number, filepath in tqdm(product(range(repeat), filepaths)):
        filepath = Path(filepath)
        filetype = filepath.suffixes[0].replace('.', '')

        for compression in compression_dict[filetype]:
            filepath = filepath.with_suffix(f'.{compression}')
            
            if filepath.is_file():
                filepath.unlink()
            _, write_time_in_seconds = time_func(
                write_and_compress_file,
                df=df,
                filepath=filepath,
                filetype=filetype,
                compression=compression
            )

            filesize_in_bytes = filepath.stat().st_size

            _, read_time_in_seconds = time_func(
                load_data,
                filepath=filepath,
                filetype=filetype,
                compression=compression
            )
            print(read_time_in_seconds)

            test_object = Test(
                test_start=test_start,
                test_category='compression',
                test_number=test_number,
                function_name= ', '.join([write_and_compress_file.__name__, load_data.__name__]),
                filepath=str(filepath),
                filetype=filetype,
                read_time_in_seconds=read_time_in_seconds,
                write_time_in_seconds=write_time_in_seconds,
                filesize_in_bytes=filesize_in_bytes,
                compression=compression
            )

            results.append(dataclasses.asdict(test_object))

    return results

if __name__ == '__main__':
    compression_dict = {}
    for filetype in FILETYPES:
        if filetype == 'parquet':
            compression_dict[filetype] = ['none', 'zstd']
        elif filetype == 'orc':
            compression_dict[filetype] = ['uncompressed', 'zstd']
        elif filetype == 'feather': 
            compression_dict[filetype] = ['uncompressed', 'zstd']
        elif filetype == 'csv':
            compression_dict[filetype] = ['uncompressed', 'zstd']
    df = csv.read_csv(ORIGINAL_CSV_FILE).to_pandas() 

    r = compression_test(df, PATHS, compression_dict)
    print(r)
    