from src.entities.pipeline_run import PipelineRun
from src.extraction.loader import load_index
from src.settings import Indexes


def load_pipeline_runs(
    dump_path: str,
    index: str,
    pipelines: dict,
    problems: dict,
    should_enforce_digest: bool,
) -> dict:
    """
    Loads a map of pipeline runs from the dump_path.
    Returns a dictionary map of each pipeline run digest to its pipeline run.
    
    Parameters
    ----------
    index : str
        The name of the pipeline_run index to load from. Should be a member of the settings.INDEXES enum.
    pipelines : Dict[str,Pipeline]
        The map of pipeline digests to pipelines returned by extraction.pipelines.load_pipelines
    problems : Dict[str,Problem]
        The mpa of problem digests to problems returned by extraction.pipelines.load_problems
    should_enforce_digest : bool
        Whether an error should be thrown if a pipeline run doesn't contain a digest
    """
    pipeline_runs = {}

    for pipeline_run_dict in load_index(dump_path, index):
        pipeline_run = PipelineRun(pipeline_run_dict, should_enforce_digest)

        # Dereference this pipeline run's pipeline and problem.
        pipeline_run.pipeline = pipelines[pipeline_run.pipeline.digest]
        pipeline_run.problem = problems[pipeline_run.problem.digest]

        pipeline_runs[pipeline_run.digest] = pipeline_run

    return pipeline_runs
