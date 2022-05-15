from filebench.constants import PATHS, ORIGINAL_CSV_FILE, FILETYPES
from filebench.write_test import write_test
from filebench.query_test import query_test
from filebench.memory_test import memory_test
from filebench.compression_test import compression_test

import pandas as pd
from pathlib import Path
from filebench.models import Test
import dataclasses
import pyarrow.csv as csv

write = True
query = True
memory = True
compression = True
data_file = Path('testdata.csv')


def main():
    if not data_file.is_file():
        column_names = [field.name for field in dataclasses.fields(Test)]
        odf = pd.DataFrame([], columns=column_names)
        odf.to_csv(data_file, index=False)

    original_df = csv.read_csv(ORIGINAL_CSV_FILE).to_pandas()

    if write:
        write_result = write_test(original_df, PATHS, repeat=10)
        write_df = pd.DataFrame(write_result)
        write_df.to_csv(data_file, mode='a', index=False, header=False)
    
    if query:
        query_result = query_test(PATHS, repeat=10)
        query_df = pd.DataFrame(query_result)
        query_df.to_csv(data_file, mode='a', index=False, header=False)
    
    if memory:
        memory_result = memory_test(PATHS, repeat=100)
        memory_df = pd.DataFrame(memory_result)
        memory_df.to_csv(data_file, mode='a', index=False, header=False)
    
    if compression:
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

        compression_result = compression_test(original_df, PATHS, compression_dict, repeat=10)
        compression_df = pd.DataFrame(compression_result)
        compression_df.to_csv(data_file, mode='a', index=False, header=False)


if __name__ == '__main__':
    main()

