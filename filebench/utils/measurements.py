from timeit import default_timer as timer
from datetime import timedelta
import psutil
import os
import gc
import time


def time_func(func, *args, **kwargs):
    start = timer()
    result = func(*args, **kwargs)
    end = timer()
    total_in_seconds = end - start
    return result, total_in_seconds


def get_process_memory():
    current_pid = os.getpid()
    process = psutil.Process(current_pid)
    rss = process.memory_info().rss
    return rss


def memory_func(func, *args, **kwargs):
    gc.collect()
    gc.disable()
    time.sleep(5)
    pre_mem = get_process_memory()
    result = func(*args, **kwargs)
    post_mem = get_process_memory()
    consumed_mem = post_mem - pre_mem
    consumed_mem_MB = consumed_mem / (1024 ** 2)
    print(f'{func.__name__}: Consumed: {consumed_mem_MB} MB.')
    gc.enable()

    return result, consumed_mem_MB