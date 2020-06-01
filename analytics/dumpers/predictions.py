import json
import os

from elasticsearch_dsl import Search
from tqdm import tqdm

from analytics.misc.settings import elasticsearch_fields, Index, Const, DataDir
from analytics.databases.d3m_client import d3m_client


def dump_predictions(batch_size: int):
    """
    Read the DB's pipeline run predictions and dump
    each one to its own file.
    """

    if not os.path.isdir(DataDir.PREDICTIONS_DUMP.value):
        os.makedirs(DataDir.PREDICTIONS_DUMP.value)

    for index in [Index.BAD_PIPELINE_RUNS, Index.PIPELINE_RUNS]:

        index_name = index.value

        s = Search(using=d3m_client, index=index_name)
        num_docs_in_index = s.count()

        # A list of specific fields to return has been provided.
        s = s.source(elasticsearch_fields[Const.PREDICTIONS.value])

        print(
            (
                f"Now dumping {num_docs_in_index} predictions documents from "
                f"'{index_name}' to dir '{DataDir.PREDICTIONS_DUMP.value}'"
            )
        )

        for hit in tqdm(s.params(size=batch_size).scan(), total=num_docs_in_index):
            out_name = os.path.join(DataDir.PREDICTIONS_DUMP.value, f"{hit.id}.json")
            with open(out_name, "w") as f:
                json.dump(hit.to_dict(), f)
