from src.entities.pipeline_run import PipelineRun
from src.extraction.loader import load_index
from src.settings import Indexes


def load_pipeline_runs(
    dump_path: str,
    index: str,
    pipelines: dict,
    problems: dict,
    datasets: dict,
    should_enforce_id: bool,
) -> dict:
    """
    Loads a map of pipeline runs from the dump_path.
    Returns a dictionary map of each pipeline run id to its pipeline run.
    
    Parameters
    ----------
    index : str
        The name of the pipeline_run index to load from. Should be a member of the
        settings.INDEXES enum.
    pipelines : Dict[str,Pipeline]
        The map of pipeline digests to pipelines returned by
        extraction.pipelines.load_pipelines
    problems : Dict[str,Problem]
        The map of problem digests to problems returned by
        extraction.problems.load_problems
    datasets : Dict[str,Dataset]
        The map of dataset digests to datasets returned by
        extraction.datasets.load_datasets
    should_enforce_id : bool
        Whether an error should be thrown if a pipeline run doesn't contain the
        appropriate id field
    """
    pipeline_runs = {}

    for pipeline_run_dict in load_index(dump_path, index):
        pipeline_run = PipelineRun(pipeline_run_dict, should_enforce_id)

        # Dereference this pipeline run's pipeline, problem, and datasets.
        pipeline_run.pipeline = pipelines[pipeline_run.pipeline.digest]
        pipeline_run.problem = problems[pipeline_run.problem.digest]
        for i, dataset_reference in enumerate(pipeline_run.datasets):
            pipeline_run.datasets[i] = datasets[dataset_reference.digest]

        pipeline_runs[pipeline_run.id] = pipeline_run

    return pipeline_runs
