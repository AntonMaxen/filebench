from dataclasses import dataclass
from typing import Any

@dataclass
class Test:
    test_start: str
    test_category: str
    test_number: int
    function_name: str
    filepath: str
    filetype: str
    write_time_in_seconds: float = -1.0
    read_time_in_seconds: float = -1.0
    memory_in_MB: float = -1.0
    filesize_in_bytes: int = -1
    compression: str = ''
