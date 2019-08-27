from typing import Callable, Any, Dict, List, Iterable, Iterator
import pickle
import os
import json
import glob

from src.misc.settings import DataDir


def has_path(dictionary: dict, keys: list) -> bool:
    """
    Checks to see if `dictionary` has the path that exists
    along `keys`.
    """
    ptr = dictionary
    for key in keys:
        if key in ptr:
            ptr = ptr[key]
        else:
            return False
    # We've made it through all the keys. The whole path exists.
    return True


def enforce_field(d: dict, field_name: str):
    if field_name not in d:
        raise ValueError(f"document must have a(n) {field_name}")


def file_len(fname):
    """
    Returns the number of lines in a file.
    Source:
    https://stackoverflow.com/questions/845058/how-to-get-line-count-cheaply-in-python
    """
    with open(fname) as f:
        for i, l in enumerate(f):
            pass
    return i + 1


def set_default(d: dict, key, default):
    """
    Assigns `default` to `d[key]` if dictionary `d` doesn't have
    that key yet.
    """
    if key not in d:
        d[key] = default


def get_file_size_mb(fname):
    return os.stat(fname).st_size / 1e6


def with_cache(f: Callable, refresh=False) -> Callable:
    """
    Uses function composition to return a function
    that wraps `f`. When that returned function is called,
    If `f` has been called before using the `with_cache`
    decoration, the cached result will be read from disk.
    If `f` has not been called before using the `with_cache`
    decoration, the result will be computed then persisted
    to disk at a location `with_cache` knows how to access.
    If `refresh == True`, the cache will be computed and
    persisted no matter what.

    Example usage:
    
    ```python
    def do_expensive_work():
        ...
    
    # Does the expensive work every time.
    do_expensive_work()
    
    # This version first looks in the cache for the result
    # of `do_expensive_work_smart`. Only if its not there is
    # the expensive work actually done.
    do_expensive_work_smart = with_cache(do_expensive_work)
    do_expensive_work_smart()
    ```

    **Note**: This caching mechanism is primitive. Only one result
    will be persisted per function. If `f` is called again with
    different arguments, the previously cached result of `f` will
    still be returned.
    """

    def f_with_cache(*args, **kwargs) -> Any:
        cache_dir = os.path.join(DataDir.CACHE.value, f.__module__)
        cache_path = os.path.join(cache_dir, f"{f.__name__}.pkl")
        if not refresh and os.path.exists(cache_path):
            # There is already a cached result for this function.
            # Return that.
            with open(cache_path, "rb") as rf:
                return pickle.load(rf)
        else:
            # Either there is no cached result for this function, or the
            # caller wants to refresh it, so compute it, cache it, then
            # return it.
            result = f(*args, **kwargs)
            # cached results are persisted in binary at location
            # "<DataDir.CACHE.value>/<module_of_f>/<name_of_f>.pkl"
            if not os.path.isdir(cache_dir):
                os.makedirs(cache_dir)
            with open(cache_path, "wb") as wf:
                pickle.dump(result, wf)
                cache_result_mb = get_file_size_mb(cache_path)
                print(
                    f"Cached result of '{f.__name__}' call to '{cache_path}' ({cache_result_mb}MB)"
                )
            return result

    return f_with_cache


def process_json(path: str, processor: Callable, *args, **kwargs) -> Any:
    """
    The loaded json will be passed to `processor` as the first arg,
    followed by any supplied *args or **kwargs.
    """
    with open(path, "r") as f:
        return processor(json.load(f), *args, **kwargs)


def process_json_glob(
    glob_pattern: str, processor: Callable, *args, **kwargs
) -> Iterator:
    """
    Goes to all files matching `glob_pattern` and
    tries to load them and pass them each through
    `processor`, supplying any additional *args and
    **kwargs supplied. Returns each processed item
    in a generator.
    """
    for path in glob.glob(glob_pattern):
        yield process_json(path, processor, *args, **kwargs)


def seq_to_map(sequence: Iterable, attr_name) -> Dict[Any, Any]:
    """
    Reduces a sequence into a map of keys to sequence items,
    where each item is indexed by the value of its attribute
    identified by `attr_name`.
    """
    return {getattr(item, attr_name): item for item in sequence}
