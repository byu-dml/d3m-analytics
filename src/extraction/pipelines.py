from src.entities.pipeline import Pipeline
from src.extraction.loader import load_index
from src.settings import Indexes


def load_pipelines(dump_path: str, should_enforce_digest: bool) -> dict:
    """
    Loads a map of pipelines from the dump_path.
    Returns a dictionary map of each pipeline digest to its pipeline.
    """
    pipelines = {}

    # First load all the pipelines
    for pipeline_dict in load_index(dump_path, Indexes.PIPELINES.value):
        pipeline = Pipeline(pipeline_dict, should_enforce_digest)
        pipelines[pipeline.digest] = pipeline

    # Now go back through and dereference any sub-pipelines
    # i.e. make the actual subpipeline to live inside its
    # parent pipeline, rather than having the parent just store
    # its digest.
    for pipeline in pipelines.values():
        pipeline.dereference_subpipelines(pipelines)

    return pipelines
