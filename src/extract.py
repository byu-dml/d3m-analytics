import pickle
from argparse import ArgumentParser
import os
from typing import Dict

from src.extraction.index_loader import load_entity_map
from src.misc.settings import DataDir, DefaultFile
from src.entities.pipeline import Pipeline
from src.entities.problem import Problem
from src.entities.dataset import Dataset
from src.entities.pipeline_run import PipelineRun
from src.misc.settings import Index
from src.misc.utils import get_file_size_mb


def get_parser() -> ArgumentParser:
    """
    Configures the argument parser for extracting and pickling the
    pipeline runs in a form ready for analysis.
    """
    parser = ArgumentParser(
        description=(
            "Make a denormalized extraction of all pipeline runs, "
            "ready for analysis."
        )
    )
    parser.add_argument(
        "--index-name",
        "-i",
        default=Index.BAD_PIPELINE_RUNS.value,
        choices=[Index.BAD_PIPELINE_RUNS.value, Index.PIPELINE_RUNS.value],
        help="The name of the dumped elasticsearch index to use as pipeline runs",
    )
    return parser


def extract_denormalized(pipeline_runs_index_name: str):
    """
    Extracts and persists a map of pipeline run digests to their companion pipeline
    runs. Each pipeline run is fully denormalized, meaning it contains references
    to its dependent pipeline (each with their dependent subpipelines) and
    to its problem.
    """
    pipeline_runs_index = Index(pipeline_runs_index_name)
    entity_maps: Dict[str, dict] = {}

    # Load and construct each entity

    entity_maps["pipelines"] = load_entity_map(Index.PIPELINES, Pipeline)

    entity_maps["problems"] = load_entity_map(Index.PROBLEMS, Problem)

    entity_maps["datasets"] = load_entity_map(Index.DATASETS, Dataset)

    entity_maps["pipeline_runs"] = load_entity_map(pipeline_runs_index, PipelineRun)

    # Next post-init each entity, now that all the entity_maps
    # are available.

    for entity_map in entity_maps.values():
        for entity in entity_map.values():
            entity.post_init(entity_maps)

    # Finally persist the entity_maps

    out_name = os.path.join(DataDir.EXTRACTION.value, DefaultFile.EXTRACTION_PKL.value)
    print(f"Now saving `entity_maps` to '{out_name}'...")

    if not os.path.isdir(DataDir.EXTRACTION.value):
        os.makedirs(DataDir.EXTRACTION.value)

    with open(out_name, "wb") as f:
        pickle.dump(entity_maps, f)

    file_size_mb = get_file_size_mb(out_name)
    print(f"Extraction successful. Wrote {file_size_mb}MB to disk.")


if __name__ == "__main__":
    parser = get_parser()
    args = parser.parse_args()
    extract_denormalized(args.index_name)
