import pickle
from argparse import ArgumentParser
import os

from src.extraction.pipeline_runs import load_pipeline_runs
from src.extraction.pipelines import load_pipelines
from src.extraction.problems import load_problems
from src.settings import DefaultDirs, DefaultFiles


def get_parser() -> ArgumentParser:
    """
    Configures the argument parser for extracting and pickling the
    pipeline runs in a form ready for analysis.
    """
    parser = ArgumentParser(
        description="Make a denormalized extraction of all pipeline runs, ready for analysis."
    )
    parser.add_argument(
        "--dump-dir",
        "-d",
        default=DefaultDirs.DUMP.value,
        help="The path to the dump folder where the pipeline runs will be extracted from.",
    )
    parser.add_argument(
        "--out-dir",
        "-o",
        default=DefaultDirs.EXTRACTION.value,
        help="The path to the folder where the extracted pipelines will be pickled to.",
    )
    parser.add_argument(
        "--index-name",
        "-i",
        default="pipeline_runs_untrusted",
        help="The name of the dumped elasticsearch index to use as pipeline runs",
    )
    parser.add_argument(
        "--dont-enforce-ids",
        "-de",
        action="store_true",
        help="If present, the system will not require that each document being loaded have its appropriate id field.",
    )
    return parser


def extract_denormalized(
    dump_path: str, out_dir: str, pipeline_runs_index_name: str, should_enforce_id: bool
):
    """
    Extracts and persists a map of pipeline run digests to their companion pipeline runs.
    Each pipeline run is fully denormalized, meaning it contains references
    to its dependent pipeline (each with their dependent subpipelines) and
    to its problem.
    """
    print("Now loading pipelines...")
    pipelines = load_pipelines(dump_path, should_enforce_id)
    print("Now loading problems...")
    problems = load_problems(dump_path, should_enforce_id)
    print("Now loading pipeline runs...")
    pipelines_runs = load_pipeline_runs(
        dump_path, pipeline_runs_index_name, pipelines, problems, should_enforce_id
    )

    out_name = f"{out_dir}/{DefaultFiles.EXTRACTION_PKL.value}"
    print(f"Now saving pipeline runs to '{out_name}'...")

    if not os.path.isdir(out_dir):
        os.mkdir(out_dir)

    with open(out_name, "wb") as f:
        pickle.dump(pipelines_runs, f)

    file_size_mb = os.stat(out_name).st_size / 1e6
    print(f"Extraction successful. Wrote {file_size_mb}MB to disk.")


if __name__ == "__main__":
    parser = get_parser()
    args = parser.parse_args()
    should_enforce_id = not args.dont_enforce_ids
    extract_denormalized(
        args.dump_dir, args.out_dir, args.index_name, should_enforce_id
    )
