import json
from argparse import ArgumentParser
import datetime
import os

from elasticsearch_dsl import Search
from tqdm import tqdm

from src.client import client
from src.misc.settings import Index, DefaultDir, elasticsearch_fields


def get_parser() -> ArgumentParser:
    """Configures the argument parser for dumping the database."""
    parser = ArgumentParser(description="Make a local dump of the D3M MtL Database.")
    parser.add_argument(
        "--out-dir",
        "-o",
        default=DefaultDir.DUMP.value,
        help=(
            "The path to the folder the dump will be "
            "written to (include the folder name)"
        ),
    )
    parser.add_argument(
        "--batch-size",
        "-b",
        type=int,
        default=250,
        help="The number of records to read from the database at a time",
    )
    parser.add_argument(
        "--indexes",
        "-i",
        nargs="*",
        choices=[index.value for index in Index],
        help="Which indexes to do a dump of. If left out, all indexes will be dumped.",
    )
    return parser


def dump(out_dir: str, batch_size: int, requested_indexes: list):
    """Read the DB indexes and dump them to `out_dir`."""
    should_index_all = len(requested_indexes) == 0

    for index in Index:
        if should_index_all or index.value in requested_indexes:

            should_retrieve_subset = len(elasticsearch_fields[index]) > 0
            index_name = index.value
            out_name = f"{out_dir}/{index_name}.json"

            s = Search(using=client, index=index_name)
            num_docs_in_index = s.count()

            if should_retrieve_subset:
                # A list of specific fields to return has been provided.
                s = s.source(elasticsearch_fields[index])

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


if __name__ == "__main__":
    parser = get_parser()
    args = parser.parse_args()
    dump(args.out_dir, args.batch_size, args.indexes)
