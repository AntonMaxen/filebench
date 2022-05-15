from pathlib import Path
import pandas as pd
import pyarrow.csv as csv
import pyarrow.parquet as parquet
import pyarrow.orc as orc
import pyarrow.feather as feather
import pyarrow as pa
from filebench.utils.measurements import time_func
from itertools import product
import os
from filebench.constants import PATHS, ORIGINAL_CSV_FILE
from filebench.models import Test
import dataclasses
import datetime
# Get orignal_file


def write_file(df, filepath, filetype):
    if filetype == 'csv':
        table = pa.Table.from_pandas(df)
        csv.write_csv(table, filepath)
    elif filetype == 'parquet':
        table = pa.Table.from_pandas(df)
        parquet.write_table(table, filepath, compression='none')
    elif filetype == 'orc':
        table = pa.Table.from_pandas(df)
        orc.write_table(table, filepath, compression='uncompressed')
    elif filetype == 'feather':
        feather.write_feather(df, filepath, compression='uncompressed')


def write_test(df: pd.DataFrame, filepaths, repeat=1):
    test_start = datetime.datetime.now(datetime.timezone.utc).isoformat()

    results = []
    for test_number, filepath in product(range(repeat), filepaths):
        filepath = Path(filepath)
        filetype = filepath.suffixes[0].replace('.', '')

        if filepath.is_file():
            filepath.unlink()

        _, write_time_in_seconds = time_func(
            func=write_file,
            df=df,
            filepath=filepath,
            filetype=filetype,
        )

        filesize_in_bytes = filepath.stat().st_size

        test_object = Test(
            test_start=test_start,
            test_category='write',
            test_number=test_number,
            function_name=write_file.__name__,
            filepath=str(filepath),
            filetype=filetype,
            write_time_in_seconds=write_time_in_seconds,
            filesize_in_bytes=filesize_in_bytes,
        )

        results.append(dataclasses.asdict(test_object))

    return results


if __name__ == '__main__':
    df = csv.read_csv(ORIGINAL_CSV_FILE).to_pandas()
    r = write_test(df, PATHS)
    print(r)