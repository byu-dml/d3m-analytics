from argparse import ArgumentParser

from src.misc.settings import DefaultDir, Index
from src.dumpers.indexes import dump_indexes
from src.dumpers.predictions import dump_predictions


def get_parser() -> ArgumentParser:
    """Configures the argument parser for dumping the database."""
    parser = ArgumentParser(description="Make a local dump of the D3M MtL Database.")
    parser.add_argument(
        "--out-dir",
        "-o",
        default=DefaultDir.DUMP.value,
        help=(
            "The path to the folder the dump will be "
            "written to (include the folder name)"
        ),
    )
    parser.add_argument(
        "--batch-size",
        "-b",
        type=int,
        default=250,
        help="The number of records to read from the database at a time",
    )
    parser.add_argument(
        "--indexes",
        "-i",
        nargs="*",
        choices=[index.value for index in Index],
        help="Which indexes to do a dump of. If left out, all indexes will be dumped.",
    )
    parser.add_argument(
        "--predictions",
        "-p",
        action="store_true",
        help=(
            "If present, the system will also dump the predictions for all pipeline runs, "
            "which takes up considerabley more disk space. Note: The predictions are dumped "
            "to a subfolder of `--out-dir`."
        ),
    )
    return parser


if __name__ == "__main__":
    parser = get_parser()
    args = parser.parse_args()
    dump_indexes(args.out_dir, args.batch_size, args.indexes)
    if args.predictions:
        dump_predictions(args.out_dir, args.batch_size)
