import json
from typing import Type, Dict, Callable, Hashable, Any
import os

from tqdm import tqdm

from src.misc.utils import file_len
from src.misc.settings import Index, DataDir
from src.entities.entity import EntityWithId


def _load_index(index: Index, processor: Callable, *args, **kwargs):
    """
    Loads a single document from a dumped index,
    returning one document at a time processed by `processor`.
    Uses a generator so the entire index doesn't need to be loaded
    into memory at once.
    
    Parameters
    ----------
    index
        The name of the database index to read from
    processor
        Each document read from the index is passed through this
        callable.
    *args
        Any remaining positional args passed into `processor`.
    **kwargs
        Any remaining key-word args passed into `processor`.
    """
    index_path = os.path.join(DataDir.INDEXES_DUMP.value, f"{index.value}.json")
    num_docs = file_len(index_path)
    with tqdm(total=num_docs) as progress_bar:
        with open(index_path, "r") as f:
            while True:
                line = f.readline()
                if not line:
                    break
                data = json.loads(line)
                yield processor(data, *args, **kwargs)
                progress_bar.update(1)


def load_index_map(
    index: Index, processor: Callable, key_getter: Callable, *args, **kwargs
):
    """
    Loads an entire DB index as a map of processed documents i.e. to
    document keys identified by `key_getter`. E.g. a key-value pair
    in the returned map will look like this:
    `key_getter(processor(document)): processor(document)`.
    """
    index_map: Dict[Hashable, Any] = {}
    print(f"Now loading `{index.value}`...")
    for processed_document in _load_index(index, processor, *args, **kwargs):
        index_map[key_getter(processed_document)] = processed_document

    return index_map


def load_entity_map(index: Index, entity_sub_class: Type[EntityWithId]):
    """
    Convenience loader for loading objects inheriting
    from `EntityWithId`.
    """
    return load_index_map(index, entity_sub_class, lambda entity: entity.get_id())
