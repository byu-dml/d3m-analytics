import json
from typing import Type, Dict

from tqdm import tqdm

from src.misc.utils import file_len
from src.misc.settings import Index
from src.entities.entity import EntityWithId


def _load_index(
    dump_path: str,
    index: Index,
    entity_sub_class: Type[EntityWithId],
    should_enforce_id: bool,
):
    """
    Loads a single document from a dumped index,
    returning one document at a time instantiated as an `entity_sub_class`.
    Uses a generator so the entire index doesn't need to be loaded
    into memory at once.
    
    Parameters
    ----------
    dump_path : str
        The filepath to the dump directory (include directory name)
    index : Index
        The name of the database index to read from
    entity_sub_class: Type[EntityWithId]
        A reference to a class that inherits from EntityWithId.
        Each document read from the index is passed through its
        constructor.
    """
    file_path = f"{dump_path}/{index.value}.json"
    num_docs = file_len(file_path)
    with tqdm(total=num_docs) as progress_bar:
        with open(file_path, "r") as f:
            while True:
                line = f.readline()
                if not line:
                    break
                entity_dict = json.loads(line)
                yield entity_sub_class(entity_dict, should_enforce_id)
                progress_bar.update(1)


def load_entity_map(
    dump_path: str,
    index: Index,
    entity_sub_class: Type[EntityWithId],
    should_enforce_id: bool,
):
    entity_map: Dict[str, EntityWithId] = {}
    print(f"Now loading `{index.value}`...")
    for entity in _load_index(dump_path, index, entity_sub_class, should_enforce_id):
        entity_map[entity.get_id()] = entity

    return entity_map
