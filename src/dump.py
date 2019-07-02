import json
import argparse

from elasticsearch_dsl import Search
from tqdm import tqdm

from client import client
from settings import INDEXES

# Configure argument parser
parser = argparse.ArgumentParser(description='Make a local dump of the D3M MtL Database.')
parser.add_argument("--out-dir", "-o", default="dump", help='The path to the folder the dump will be written to (include the folder name)')
parser.add_argument("--batch-size", "-b", type=int, default=100, help='The number of records to read from the database at a time')
args = parser.parse_args()

# Read the indexes and dump them to args.out_dir
for name_key in INDEXES:
    index_name = INDEXES[name_key]
    out_name = f"{args.out_dir}/{index_name}.json"

    s = Search(using=client, index=index_name)
    num_docs_in_index = s.count()
    print(f"Now writing index '{index_name}' ({num_docs_in_index} documents) to path '{out_name}'")

    with open(out_name, "w") as f:
        for hit in tqdm(s.params(size=args.batch_size).scan(), total=num_docs_in_index):
            # Paginate over this index
            f.write(json.dumps(hit.to_dict()))
            f.write("\n")