import os
from pathlib import Path

REDDIT_PLACE = '2022_place_canvas_history'
DAY_1 = 'data/small/day1'
ORIGINAL_PATH = Path(DAY_1, 'original', REDDIT_PLACE)
ORIGINAL_CSV_FILE = ORIGINAL_PATH.with_suffix('.csv')
BASE_PATH = Path(DAY_1, REDDIT_PLACE)
FILETYPES = ['parquet', 'orc', 'feather', 'csv']
PATH_DICT = {filetype:BASE_PATH.with_suffix(f'.{filetype}') for filetype in FILETYPES}
PATHS = [PATH_DICT[filetype] for filetype in FILETYPES]

COMPRESSIONS = {
    'feather': ['lz4', 'zstd', 'uncompressed'],
    'orc': ['zlib', 'snappy', 'lz4', 'zstd', 'uncompressed'],
    'parquet': ['zstd', 'lz4', 'brotli', 'gzip', 'snappy', 'none'],
    'csv': ["infer", "gzip", "bz2", "zip", "xz", "zstd"]
}