"""
Sees how many of the exline pipelines are found in the DB
already. Saves to `MATCHES_SAVE_PATH` the digests of pipelines in
the DB that are tantamount to the Exline pipelines.
"""

from collections import defaultdict
from pprint import pprint
import json
from typing import Dict, Optional, List

from analytics.analyze import load_entity_maps_pkl
from analytics.entities.pipeline import Pipeline
from analytics.entities.pipeline_run import PipelineRun

MATCHES_SAVE_PATH = "dump/exline-ids-to-digests-of-db-equivalents.json"
RUN_MATCHES_SAVE_PATH = "dump/pipeline-run-digests-of-exlines-or-matches.json"

entity_maps = load_entity_maps_pkl()
pipelines: Dict[str, Pipeline] = entity_maps["pipelines"]
runs: Dict[str, PipelineRun] = entity_maps["pipeline_runs"]
exlines: Dict[str, Pipeline] = Pipeline.from_json_glob("dump/exlines/*.json")

# What is the distribution of source name across the pipelines
# isomorphic to the exlines?
source_name_cnt: Dict[Optional[str], int] = defaultdict(int)

tantamount_matches: defaultdict = defaultdict(list)
for exline in exlines.values():
    for pipeline in pipelines.values():
        if exline.is_tantamount_to(pipeline):
            tantamount_matches[exline.id].append(pipeline.digest)
            source_name_cnt[pipeline.source_name] += 1
            if len(tantamount_matches[exline.id]) == 0:
                print(f"\nfirst match found for exline {exline.id}.")
                print("exline steps:")
                exline.print_steps()
                print("matching pipeline steps:")
                pipeline.print_steps()


# Search for pipelines in the DB that have the same digests as
# the exlines. If the digests are the same they should be exactly
# equivalent.
print("Now looking for exact matches by digest...")
for exline in exlines.values():
    if exline.digest in pipelines:
        print(f"found exact match for exline with id={exline.id}")

print("tantamount_matches (pipelines in DB tantamount to the exlines)")
pprint(tantamount_matches)

print(
    f"The distribution of source names among tantamount matches is: {source_name_cnt}"
)

with open(MATCHES_SAVE_PATH, "w") as f:
    json.dump(tantamount_matches, f, indent=4)

print(f"tantamount_matches written to '{MATCHES_SAVE_PATH}'")

# Unpack all the match digests and exline digests into a single list
match_digests = [match for matches in tantamount_matches.items() for match in matches]
match_digests += exlines.keys()

runs_using_exlines_or_matches: List[str] = []
# Find any pipeline runs using a match or exline pipeline
for digest in match_digests:
    for run in runs.values():
        if digest == run.pipeline.get_id():
            runs_using_exlines_or_matches.append(run.get_id())

with open(RUN_MATCHES_SAVE_PATH, "w") as f:
    json.dump(runs_using_exlines_or_matches, f)
print(f"pipeline run matches written to {RUN_MATCHES_SAVE_PATH}")
