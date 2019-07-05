import pickle
from argparse import ArgumentParser
import os

from src.extraction.pipeline_runs import load_pipeline_runs
from src.extraction.pipelines import load_pipelines
from src.extraction.problems import load_problems


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
        default="dump",
        help="The path to the dump folder where the pipeline runs will be extracted from.",
    )
    parser.add_argument(
        "--out-dir",
        "-o",
        default="extractions",
        help="The path to the folder where the extracted pipelines will be pickled to.",
    )
    parser.add_argument(
        "--index-name",
        "-i",
        default="pipeline_runs_untrusted",
        help="The name of the dumped elasticsearch index to use as pipeline runs",
    )
    parser.add_argument(
        "--dont-enforce-digests",
        "-de",
        action="store_true",
        help="If present, the system will not require that each document being loaded have a digest.",
    )
    return parser


def extract_denormalized(
    dump_path: str,
    out_dir: str,
    pipeline_runs_index_name: str,
    should_enforce_digest: bool,
):
    """
    Extracts and persists a map of pipeline run digests to their companion pipeline runs.
    Each pipeline run is fully denormalized, meaning it contains references
    to its dependent pipeline (each with their dependent subpipelines) and
    to its problem.
    """
    print("Now loading pipelines...")
    pipelines = load_pipelines(dump_path, should_enforce_digest)
    print("Now loading problems...")
    problems = load_problems(dump_path, should_enforce_digest)
    print("Now loading pipeline runs...")
    pipelines_runs = load_pipeline_runs(
        dump_path, pipeline_runs_index_name, pipelines, problems, should_enforce_digest
    )

    out_name = f"{out_dir}/denormalized_pipeline_runs.pkl"
    print(f"Now saving pipeline runs to '{out_name}'...")
    if not os.path.isdir(out_dir):
        os.mkdir(out_dir)
    with open(out_name, "wb") as f:
        pickle.dump(pipelines_runs, f)


if __name__ == "__main__":
    parser = get_parser()
    args = parser.parse_args()
    should_enforce_digest = not args.dont_enforce_digests
    extract_denormalized(
        args.dump_dir, args.out_dir, args.index_name, should_enforce_digest
    )
