from typing import Callable, Any, Dict, Iterable, Iterator
import pickle
import os
import json
import glob

from pymongo.collection import Collection

from analytics.misc.settings import DataDir, DefaultFile


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
        for i, _ in enumerate(f):
            pass
    return i + 1


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


def load_entity_maps_pkl() -> Dict[str, dict]:
    read_path = os.path.join(DataDir.EXTRACTION.value, DefaultFile.EXTRACTION_PKL.value)
    print(f"Now loading pickled entity maps from '{read_path}'...")
    with open(read_path, "rb") as f:
        return pickle.load(f)


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


def seq_to_map(sequence: Iterable, key_getter: Callable) -> Dict[Any, Any]:
    """
    Reduces a sequence into a map of keys to sequence items,
    where each item is indexed by the value retrieved for it
    using `key_getter`. I.e. each entry in the resulting map
    will look like `key_getter(entry): entry`.
    """
    return {key_getter(item): item for item in sequence}


class MongoWriteBuffer:
    """
    Wraps the MongoDB `bulk_write` API to make writing
    bulk operations to a collection in batches easier.
    """

    def __init__(self, collection: Collection, batch_size: int) -> None:
        self.collection = collection
        self.batch_size = batch_size
        self.ops_buffer = []

    def queue(self, operation) -> None:
        """
        Add an operation to the queue. Once we reach the
        batch size, we automatically write and flush the buffer.
        """
        self.ops_buffer.append(operation)
        if self.__len__() >= self.batch_size:
            self.flush()

    def __len__(self) -> int:
        return len(self.ops_buffer)

    def flush(self) -> None:
        if self.__len__() > 0:
            self.collection.bulk_write(self.ops_buffer, ordered=False)
            self.ops_buffer.clear()
