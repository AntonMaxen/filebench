import pyarrow.parquet as parquet
import pyarrow.feather as feather
import pyarrow.orc as orc
import pyarrow.csv as csv
from filebench.utils.measurements import memory_func
from pathlib import Path
from itertools import product
from filebench.models import Test
import datetime
from filebench.constants import PATHS
import dataclasses


def read_table(filepath, filetype):
    if filetype == 'csv':
        table = csv.read_csv(filepath)
    elif filetype == 'parquet':
        table = parquet.read_table(filepath)
    elif filetype == 'orc':
        table = orc.read_table(filepath)
    elif filetype == 'feather':
        table = feather.read_table(filepath)

    return table


def memory_test(filepaths, repeat=1):
    test_start = datetime.datetime.now(datetime.timezone.utc).isoformat()

    results = []
    for test_number, filepath in product(range(repeat), filepaths):
        filepath = Path(filepath)
        filetype = filepath.suffixes[0].replace('.', '')

        _, memory_in_MB = memory_func(read_table, filepath, filetype)

        test_object = Test(
            test_start=test_start,
            test_category='memory',
            test_number=test_number,
            function_name=read_table.__name__,
            filepath=str(filepath),
            filetype=filetype,
            memory_in_MB=memory_in_MB,
        )


        results.append(dataclasses.asdict(test_object))
    
    return results


if __name__ == '__main__':
    r = memory_test(sorted(PATHS))
    print(r)