import functools
import glob
import hashlib
import json
import os
import pickle
import time

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from zdutil.log import timeit

# ideally we use the home directory, current directory otherwise
HOME = os.environ.get('HOME', '.')
CACHE_PATH = '/'.join((HOME, '.zdata'))
VALID_EXT = set(['parquet', 'pkl', 'feather'])


def _load_most_recent_from_cache(contains):
    """loads most recent cache key containing substring (useful for debugging/speeding up load times)"""
    if callable(contains):
        contains = contains.__name__
    files = glob.glob(f"{CACHE_PATH}/{contains}*")
    latest_file = max(files, key=os.path.getctime)
    return read_from_disk(latest_file)


def safe_json(data):
    """determines if something can be serialized to JSON"""
    if data is None:
        return True
    elif isinstance(data, (bool, int, float, str)):
        return True
    elif isinstance(data, (tuple, list)):
        return all(safe_json(x) for x in data)
    elif isinstance(data, dict):
        return all(isinstance(k, str) and safe_json(v) for k, v in data.items())
    # although raw dataframes are not generally json serializable we will hash them later
    elif isinstance(data, pd.DataFrame):
        return True
    return False


def serialize_df(o):
    if isinstance(o, pd.DataFrame):
        return int(pd.util.hash_pandas_object(o).sum())
    else:
        return o


def _hash_input(*args, **kwargs):
    # TODO: make this more general so kwargs and positional args
    # that are the same thing hash to the same key
    kwargs = {k: v for k, v in kwargs.items() if safe_json(v)}
    args = [arg for arg in args if safe_json(arg)]
    key = {'kwargs': kwargs, 'args': args}
    serialized = json.dumps(key, default=serialize_df)
    return hashlib.sha256(serialized.encode()).hexdigest()


def _cache_path(func, *args, **kwargs):
    """cache path from args"""
    return f"{CACHE_PATH}/{func.__name__}-{_hash_input(*args, **kwargs)}"


@timeit
def write_to_disk(obj, path):
    """writes file to disk"""
    ext = path.split('.')[-1]
    if ext == 'parquet':
        table = pa.Table.from_pandas(obj)
        pq.write_table(table, path)
    elif ext == 'feather':
        obj.to_feather(path)
    elif ext == 'pkl':
        with open(path, 'wb+') as f:
            pickle.dump(obj, f)
    else:
        raise Exception('bad ext.')


@timeit(wargs=True)
def read_from_disk(path):
    """reads file from disk"""
    ext = path.split('.')[-1]
    if ext == 'parquet':
        table = pq.read_table(path)
        result = table.to_pandas()
    elif ext == 'feather':
        result = pd.read_feather(path)
    elif ext == 'pkl':
        with open(path, 'rb+') as f:
            result = pickle.load(f)
    else:
        raise Exception('bad ext.')
    return result


def disk_cache(func=None, *, max_age='3D', ext='parquet'):
    """
    decorator to cache functions to disk using hash of input arguments

    :param func: function run/result to cache
    :param max_age: maximum age (this is in the style of pandas timedelta so (1S, 1M, 1H, 1D, etc)
    :param ext: file extension to use/format to write to disk (parquet, feather, or pickle)
    :return: function result
    """
    if ext not in VALID_EXT:
        raise Exception(f"extension {ext} not supported")

    if func is None:
        return functools.partial(disk_cache, max_age=max_age, ext=ext)

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        path = _cache_path(func, *args, **kwargs) + '.' + ext
        if not os.path.exists(path):
            if not os.path.exists(CACHE_PATH):
                os.makedirs(CACHE_PATH)
            result = func(*args, **kwargs)
            write_to_disk(result, path)
        else:
            age = time.time() - os.path.getmtime(path)
            if age < pd.to_timedelta(max_age).total_seconds():
                result = read_from_disk(path)
            else:
                print(f'too old, deleting from disk: {path}')
                os.remove(path)
                result = wrapper(*args, **kwargs)
        return result

    return wrapper
