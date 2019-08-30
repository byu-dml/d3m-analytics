from argparse import ArgumentParser
import os
import subprocess

from src.misc.settings import DataDir, Index


def get_parser() -> ArgumentParser:
    """Configures the argument parser for inspecting a database dump."""
    parser = ArgumentParser(
        description="Inspect records that have been dumped from the DB."
    )
    parser.add_argument(
        "--entity-id",
        "-id",
        type=str,
        help=(
            "The id of the entity to inspect. If left out, the first entity in "
            "`--index` will be looked up."
        ),
    )
    parser.add_argument(
        "--index",
        "-i",
        type=str,
        default=Index.BAD_PIPELINE_RUNS.value,
        choices=[index.value for index in Index],
        help="Which index to look in for the `--entity-id`.",
    )
    parser.add_argument(
        "--predictions",
        "-p",
        action="store_true",
        help=(
            "If present, it is assumed `--index` is a pipeline run index and "
            "the predictions for the pipeline run identified by `--entity-id` "
            "will also be looked up."
        ),
    )
    return parser


if __name__ == "__main__":
    parser = get_parser()
    args = parser.parse_args()

    if args.predictions and not args.entity_id:
        parser.error("choosing --predictions requires an --entity-id to be supplied")

    chosen_index = Index(args.index)
    index_path = os.path.join(DataDir.INDEXES_DUMP.value, f"{chosen_index.value}.json")

    if args.entity_id:
        print(
            "****\n"
            f"Searching for document in '{DataDir.INDEXES_DUMP.value}' with id='{args.entity_id}'...\n"
            "****"
        )
        subprocess.run(["grep", args.entity_id, index_path])
    else:
        print("****\n" f"First document of '{DataDir.INDEXES_DUMP.value}':\n" "****")
        subprocess.run(["head", "-1", index_path])

    if args.predictions:
        predictions_path = os.path.join(
            DataDir.PREDICTIONS_DUMP.value, f"{args.entity_id}.json"
        )
        print("****\n" f"Searching for predictions at '{predictions_path}'...\n" "****")
        subprocess.run(["cat", predictions_path])
        print()
