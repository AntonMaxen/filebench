from ast import Call
import pyarrow.parquet as parquet
import pyarrow.orc as orc
import pyarrow.feather as feather
import pyarrow.csv as csv
import pyarrow as pa
import pandas as pd
from itertools import product
from typing import List, Tuple, Dict, Callable
from pathlib import Path
from filebench.utils.measurements import time_func
from filebench.models import Test
import datetime
import dataclasses
from filebench.constants import PATHS


def entire_file_to_df(filepath, filetype):
    """Read entire file convert df and get shape"""

    if filetype == 'parquet':
        df = parquet.read_table(filepath).to_pandas()
    elif filetype == 'orc':
        df = orc.read_table(filepath).to_pandas()
    elif filetype == 'feather':
        df = feather.read_feather(filepath)
    elif filetype == 'csv':
        df = csv.read_csv(filepath).to_pandas()
    
    return df.shape


def get_amount_participants(filepath, filetype):
    """Get amount of unique participants using pyarrow and pandas (High cardinality)."""

    if filetype == 'parquet':
        df = parquet.read_table(filepath, columns=['user_id']).to_pandas()
    elif filetype == 'orc':
        df = orc.read_table(filepath, columns=['user_id']).to_pandas()
    elif filetype == 'feather':
        df = feather.read_feather(filepath, columns=['user_id'])
    elif filetype == 'csv':
        df = csv.read_csv(filepath).to_pandas()
    
    return pd.unique(df['user_id']).shape[0]


def get_amount_participants_optimized(filepath, filetype):
    """Get amount of unique participants optimized only pyarrow."""

    if filetype == 'parquet':
        table = parquet.read_table(filepath, columns=['user_id'])
    elif filetype == 'orc':
        table = orc.read_table(filepath, columns=['user_id'])
    elif filetype == 'feather':
        table = feather.read_table(filepath, columns=['user_id'])
    elif filetype == 'csv':
        table = csv.read_csv(filepath)
    
    return len(table['user_id'].unique())


def get_amount_colors_used(filepath, filetype):
    """Get amount unique colors used using pyarrow and pandas (low cardinality)."""

    if filetype == 'parquet':
        table = parquet.read_table(filepath, columns=['pixel_color'])
    elif filetype == 'orc':
        table = orc.read_table(filepath, columns=['pixel_color'])
    elif filetype == 'feather':
        table = feather.read_table(filepath, columns=['pixel_color'])
    elif filetype == 'csv':
        table = csv.read_csv(filepath)
    
    return len(table['pixel_color'].unique())


def get_amount_colors_used_optimized(filepath, filetype):
    """Get amount unique colors used optimized only pyarrow (low cardinality)."""

    if filetype == 'parquet':
        table = parquet.read_table(filepath, columns=['pixel_color'])
    elif filetype == 'orc':
        table = orc.read_table(filepath, columns=['pixel_color'])
    elif filetype == 'feather':
        table = feather.read_table(filepath, columns=['pixel_color'])
    elif filetype == 'csv':
        table = csv.read_csv(filepath)
    
    return len(table['pixel_color'].unique())


def get_color_counts_of_all_colors(filepath, filetype):
    """Get color counts of all colors used. (Aggregation)"""

    if filetype == 'parquet':
        table = parquet.read_table(filepath, columns=['pixel_color'])
        value_counts = pa.compute.value_counts(table['pixel_color'].combine_chunks()).flatten()
        result = pd.Series(value_counts[1], value_counts[0])


def get_number_of_rows(filepath, filetype):        
    """Row count"""

    if filetype == 'parquet':
        num_rows = parquet.read_metadata(filepath).num_rows
    elif filetype == 'orc':
        num_rows = orc.ORCFile(filepath).nrows
    elif filetype == 'feather':
        num_rows = feather.read_table(filepath).num_rows
    elif filetype == 'csv':
        num_rows = csv.read_csv(filepath).num_rows
    
    return num_rows


def read_pyarrow_to_pandas(filepath, filetype):
    """Read file to pyarrow table and convert to pandas dataframe"""

    if filetype == 'parquet':
        df = parquet.read_table(filepath).to_pandas()
    elif filetype == 'orc':
        df = orc.read_table(filepath).to_pandas()
    elif filetype == 'feather':
        df = feather.read_feather(filepath)
    elif filetype == 'csv':
        df = csv.read_csv(filepath).to_pandas()
    
    return df.shape


def read_pandas(filepath, filetype):
    """Read file to pandas dataframe"""

    if filetype == 'parquet':
        df = pd.read_parquet(filepath)
    elif filetype == 'orc':
        df = pd.read_orc(filepath)
    elif filetype == 'feather':
        df = pd.read_feather(filepath)
    elif filetype == 'csv':
        df = pd.read_csv(filepath)
    
    return df.shape


def query_test(filepaths: List[str], repeat: int = 1):
    test_start = datetime.datetime.now(datetime.timezone.utc).isoformat()
    FUNCTIONS = [
        entire_file_to_df,
        get_amount_participants,
        get_amount_colors_used,
        get_number_of_rows,
        read_pandas,
        read_pyarrow_to_pandas
    ]

    results = []
    for test_number, func, filepath  in product(range(repeat), FUNCTIONS, filepaths):
        filepath = Path(filepath)
        filetype = filepath.suffixes[0].replace('.', '')
        print(f'Running {func.__name__}')
        result = time_func(
            func=func,
            filepath=filepath, 
            filetype=filetype
        )

        test_object = Test(
            test_start=test_start,
            test_category='read',
            test_number=test_number,
            function_name=func.__name__,
            filepath=str(filepath),
            filetype=filetype,
            read_time_in_seconds=result[1]
        )
        
        results.append(dataclasses.asdict(test_object))

    return results


if __name__ == '__main__':

    result = query_test(PATHS)
    print(result)