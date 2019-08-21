"""
Looks at all the Exline pipelines found using
`exline_glob_pattern` to see if there are any
pipelines that are, for all intents and purposes,
equivalent, even though they have different ids.
"""

import glob
from typing import List
import json
import itertools

from src.entities.pipeline import Pipeline


exline_glob_pattern = "dump/exlines/*.json"
pipelines = Pipeline.from_json_glob(exline_glob_pattern, True)

for pipe_a, pipe_b in itertools.combinations(pipelines, 2):
    if pipe_a.is_tantamount_to(pipe_b):
        # tantamount here means they are the same in all ways that matter.
        print(f"pipeline id={pipe_a.id} is tantamount to pipeline id={pipe_b.id}")

