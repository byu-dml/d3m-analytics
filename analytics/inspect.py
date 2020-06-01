from argparse import ArgumentParser
import os
import subprocess

from analytics.misc.settings import DataDir, Index


def get_parser() -> ArgumentParser:
    """Configures the argument parser for inspecting a database dump."""
    parser = ArgumentParser(
        description="Inspect records that have been dumped from the DB."
    )
    parser.add_argument(
        "--index",
        "-i",
        type=str,
        default=Index.BAD_PIPELINE_RUNS.value,
        choices=[index.value for index in Index],
        help="Which index to write to stdout for grepping, etc.",
    )
    parser.add_argument(
        "--predictions-id",
        "-p",
        type=str,
        help=(
            "If present, the predictions identified by `--predictions-id` will be served up."
        ),
    )
    return parser


if __name__ == "__main__":
    parser = get_parser()
    args = parser.parse_args()

    chosen_index = Index(args.index)
    index_path = os.path.join(DataDir.INDEXES_DUMP.value, f"{chosen_index.value}.json")

    if args.predictions_id:
        # Write the predictions document having `args.predictions_id` to stdout.
        predictions_path = os.path.join(
            DataDir.PREDICTIONS_DUMP.value, f"{args.entity_id}.json"
        )
        print("****\n" f"Searching for predictions at '{predictions_path}'...\n" "****")
        subprocess.run(["cat", predictions_path])
        print()
    else:
        # Write the index identified by `args.index` to stdout.
        subprocess.run(["cat", index_path])
