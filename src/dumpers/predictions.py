import json
import os

from elasticsearch_dsl import Search
from tqdm import tqdm

from src.misc.settings import elasticsearch_fields, Index, Const
from src.client import client


def dump_predictions(base_out_dir: str, batch_size: int):
    """
    Read the DB's pipeline run predictions and dump
    each one to its own file in `out_dir`.
    """
    for index in [Index.BAD_PIPELINE_RUNS, Index.PIPELINE_RUNS]:

        index_name = index.value
        out_dir = f"{base_out_dir}/predictions/{index_name}"

        s = Search(using=client, index=index_name)
        num_docs_in_index = s.count()

        # A list of specific fields to return has been provided.
        s = s.source(elasticsearch_fields[Const.PREDICTIONS.value])

        print(
            (
                f"Now writing {num_docs_in_index} predictions documents from '{index_name}' "
                f"to dir '{out_dir}'"
            )
        )

        if not os.path.isdir(out_dir):
            os.makedirs(out_dir)

        for hit in tqdm(s.params(size=batch_size).scan(), total=num_docs_in_index):
            out_name = f"{out_dir}/{hit.id}.json"
            with open(out_name, "w") as f:
                json.dump(hit.to_dict(), f)
