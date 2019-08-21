"""
Sees how many of the exline pipelines are found in the DB
already. Saves to `SAVE_PATH` the digests of pipelines in
the DB that are tantamount to the Exline pipelines.
"""

from collections import defaultdict
from pprint import pprint
import json
from typing import Dict

from src.analyze import load_entity_maps_pkl
from src.entities.pipeline import Pipeline

SAVE_PATH = "dump/exline-ids-to-digests-of-db-equivalents.json"

pipelines: Dict[str, Pipeline] = load_entity_maps_pkl()["pipelines"]
exlines: Dict[str, Pipeline] = Pipeline.from_json_glob("dump/exlines/*.json", True)

tantamount_matches: defaultdict = defaultdict(list)
for exline in exlines.values():
    for pipeline in pipelines.values():
        if exline.is_tantamount_to(pipeline):
            if len(tantamount_matches[exline.id]) == 0:
                print(f"\nfirst match found for exline {exline.id}.")
                print("exline steps:")
                exline.print_steps()
                print("matching pipeline steps:")
                pipeline.print_steps()
            tantamount_matches[exline.id].append(pipeline.digest)

# Search for pipelines in the DB that have the same digests as
# the exlines. If the digests are the same they should be exactly
# equivalent.
exact_matches: defaultdict = defaultdict(list)
for exline in exlines.values():
    if exline.digest in pipelines:
        exact_matches[exline.id].append(pipeline.digest)

print("tantamount_matches (pipelines in DB tantamount to the exlines)")
pprint(tantamount_matches)

print("exact_matches (pipelines in DB with same digest as the exlines)")
pprint(exact_matches)

with open(SAVE_PATH, "w") as f:
    json.dump(tantamount_matches, f, indent=4)

print(f"tantamount_matches written to '{SAVE_PATH}'")
