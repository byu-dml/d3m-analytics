import json
from typing import List, Dict

from src.entities.pipeline import Pipeline

test_pipeline_names: List[str] = [
    "simple_pipeline_a",
    "simple_pipeline_b",
    "pipeline_with_subpipelines",
    "simple_pipeline_b_one_off",
]

_test_pipelines_path: str = "./tests/data"
_test_pipelines_ext: str = ".json"


def load_test_pipelines() -> Dict[str, Pipeline]:
    """
    Returns mapping of each pipeline's digest to the pipeline.
    """
    pipelines: Dict[str, Pipeline] = {}

    for name in test_pipeline_names:
        with open(f"{_test_pipelines_path}/{name}{_test_pipelines_ext}", "r") as f:
            pipeline_dict: dict = json.load(f)
            pipeline = Pipeline(pipeline_dict)
            pipelines[pipeline.digest] = pipeline

    for pipeline in pipelines.values():
        pipeline.dereference_subpipelines(pipelines)

    return pipelines
