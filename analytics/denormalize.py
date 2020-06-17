from fire import Fire
from pymongo.operations import UpdateMany

from analytics.misc.settings import Index
from analytics.databases.aml_client import AMLDB


def denormalize_problem_on_all_runs(problem: dict) -> UpdateMany:
    """
    Finds all pipeline runs whose problem reference matches this
    problem, and replaces the reference with a copy of the actual
    problem document.
    """
    return UpdateMany(
        filter={
            "$and": [
                {"problem.id": problem["id"]},
                {"problem.digest": problem["digest"]},
            ]
        },
        update={"$set": {"problem": problem}},
    )


def denormalize_dataset_on_all_runs(dataset: dict) -> UpdateMany:
    """
    Finds all pipeline runs having a dataset reference matches this
    dataset, and replaces the reference with a copy of the actual
    dataset document.
    """
    return UpdateMany(
        filter={
            "$and": [
                {"datasets.id": dataset["id"]},
                {"datasets.digest": dataset["digest"]},
            ]
        },
        update={"$set": {"datasets.$[matcher]": dataset}},
        array_filters=[
            {"matcher.id": dataset["id"], "matcher.digest": dataset["digest"]}
        ],
    )


def denormalize_pipeline_on_all_runs(pipeline: dict) -> UpdateMany:
    """
    Finds all pipeline runs whose pipeline reference matches this
    pipeline, and replaces the reference with a copy of the actual
    pipeline document.
    """
    return UpdateMany(
        filter={
            "$and": [
                {"pipeline.id": pipeline["id"]},
                {"pipeline.digest": pipeline["digest"]},
            ]
        },
        update={"$set": {"pipeline": pipeline}},
    )


def extract_denormalized(batch_size: int = 100) -> None:
    """
    Denormalizes all pipeline run documents in the lab's local
    database. Adds a copy of each run's problem, datasets, and
    pipeline(s) to the pipeline run document. Reads and updates
    in batches of size `batch_size`.
    """
    aml = AMLDB()

    print("denormalizing all pipeline run problem references...")
    aml.bulk_read_write(
        Index.PROBLEMS, Index.PIPELINE_RUNS, denormalize_problem_on_all_runs, batch_size
    )
    print("denormalizing all pipeline run dataset references...")
    aml.bulk_read_write(
        Index.DATASETS, Index.PIPELINE_RUNS, denormalize_dataset_on_all_runs, batch_size
    )
    print("denormalizing all pipeline run pipeline references...")
    aml.bulk_read_write(
        Index.PIPELINES,
        Index.PIPELINE_RUNS,
        denormalize_pipeline_on_all_runs,
        batch_size,
    )


if __name__ == "__main__":
    Fire(extract_denormalized)
