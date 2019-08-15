import pickle
from argparse import ArgumentParser
import os
from typing import Dict

from src.extraction.loader import load_entity_map
from src.misc.settings import DefaultDir, DefaultFile
from src.entities.pipeline import Pipeline
from src.entities.problem import Problem
from src.entities.dataset import Dataset
from src.entities.pipeline_run import PipelineRun
from src.misc.settings import Index


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
        "--dump-dir",
        "-d",
        default=DefaultDir.DUMP.value,
        help=(
            "The path to the dump folder where the pipeline runs will be "
            "extracted from."
        ),
    )
    parser.add_argument(
        "--out-dir",
        "-o",
        default=DefaultDir.EXTRACTION.value,
        help="The path to the folder where the extracted pipelines will be pickled to.",
    )
    parser.add_argument(
        "--index-name",
        "-i",
        default=Index.BAD_PIPELINE_RUNS.value,
        choices=[Index.BAD_PIPELINE_RUNS.value, Index.PIPELINE_RUNS.value],
        help="The name of the dumped elasticsearch index to use as pipeline runs",
    )
    parser.add_argument(
        "--dont-enforce-ids",
        "-de",
        action="store_true",
        help=(
            "If present, the system will not require that each document being "
            "loaded have its appropriate id field."
        ),
    )
    return parser


def extract_denormalized(
    dump_path: str, out_dir: str, pipeline_runs_index_name: str, should_enforce_id: bool
):
    """
    Extracts and persists a map of pipeline run digests to their companion pipeline
    runs. Each pipeline run is fully denormalized, meaning it contains references
    to its dependent pipeline (each with their dependent subpipelines) and
    to its problem.
    """
    pipeline_runs_index = Index(pipeline_runs_index_name)
    entity_maps: Dict[str, dict] = {}

    # Load and construct each entity

    entity_maps["pipelines"] = load_entity_map(
        dump_path, Index.PIPELINES, Pipeline, should_enforce_id
    )

    entity_maps["problems"] = load_entity_map(
        dump_path, Index.PROBLEMS, Problem, should_enforce_id
    )

    entity_maps["datasets"] = load_entity_map(
        dump_path, Index.DATASETS, Dataset, should_enforce_id
    )

    entity_maps["pipeline_runs"] = load_entity_map(
        dump_path, pipeline_runs_index, PipelineRun, should_enforce_id
    )

    # Next post-init each entity, now that all the entity_maps
    # are available.

    for entity_map in entity_maps.values():
        for entity in entity_map.values():
            entity.post_init(entity_maps)

    # Finally persist the entity_maps

    out_name = f"{out_dir}/{DefaultFile.EXTRACTION_PKL.value}"
    print(f"Now saving `entity_maps` to '{out_name}'...")

    if not os.path.isdir(out_dir):
        os.mkdir(out_dir)

    with open(out_name, "wb") as f:
        pickle.dump(entity_maps, f)

    file_size_mb = os.stat(out_name).st_size / 1e6
    print(f"Extraction successful. Wrote {file_size_mb}MB to disk.")


if __name__ == "__main__":
    parser = get_parser()
    args = parser.parse_args()
    should_enforce_id = not args.dont_enforce_ids
    extract_denormalized(
        args.dump_dir, args.out_dir, args.index_name, should_enforce_id
    )
