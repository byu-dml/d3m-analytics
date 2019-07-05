import json

from tqdm import tqdm

from src.utils import file_len


def load_index(dump_path: str, index_name: str):
    """
    Loads a single document from a dumped index,
    returning one document at a time in dictionary format.
    
    Parameters
    ----------
    dump_path : str
        The filepath to the dump directory (include directory name)
    index_name : str
        The name of the database index to read from
    """
    file_path = f"{dump_path}/{index_name}.json"
    num_docs = file_len(file_path)
    with tqdm(total=num_docs) as pbar:
        with open(file_path, "r") as f:
            while True:
                line: str = f.readline()
                if not line:
                    break
                yield json.loads(line)
                pbar.update(1)
