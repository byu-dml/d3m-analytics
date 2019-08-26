import json
import os

from elasticsearch_dsl import Search
from tqdm import tqdm

from src.misc.settings import elasticsearch_fields, Index
from src.client import client


def dump_indexes(out_dir: str, batch_size: int, requested_indexes: list):
    """Read the DB indexes and dump them to `out_dir`."""
    should_index_all = requested_indexes is None

    for index in Index:
        if should_index_all or index.value in requested_indexes:

            should_retrieve_subset = len(elasticsearch_fields[index.value]) > 0
            index_name = index.value
            out_name = f"{out_dir}/{index_name}.json"

            s = Search(using=client, index=index_name)
            num_docs_in_index = s.count()

            if should_retrieve_subset:
                # A list of specific fields to return has been provided.
                s = s.source(elasticsearch_fields[index.value])

            print(
                (
                    f"Now writing index '{index_name}' ({num_docs_in_index} documents) "
                    f"to path '{out_name}'"
                )
            )

            if not os.path.isdir(out_dir):
                os.mkdir(out_dir)

            with open(out_name, "w") as f:
                for hit in tqdm(
                    s.params(size=batch_size).scan(), total=num_docs_in_index
                ):
                    # Paginate over this index
                    f.write(json.dumps(hit.to_dict()))
                    f.write("\n")
