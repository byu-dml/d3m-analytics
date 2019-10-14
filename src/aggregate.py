from argparse import ArgumentParser
from typing import Type, Mapping, Dict

from src.misc.utils import load_entity_maps_pkl
from src.aggregations.aggregation import Aggregation
from src.aggregations.primitive_pairs import PrimitivePairComparisonAggregation


aggregation_map: Mapping[str, Type[Aggregation]] = {
    "primitive_pairs": PrimitivePairComparisonAggregation
}

def get_parser() -> ArgumentParser:
    """
    Configures the argument parser for aggregating a pickled extraction of pipeline runs.
    """
    parser = ArgumentParser(
        description="Load and aggregate a pickled extraction of pipeline runs"
    )
    parser.add_argument(
        "aggregation",
        choices=aggregation_map.keys(),
        help="The name of the aggregation to conduct"
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help=(
            "If present, the aggregation will output detailed information on its "
            "progress and intermediate results during computation, if applicable"
        ),
    )
    parser.add_argument(
        "--refresh",
        "-r",
        action="store_true",
        help=("If present, any caches that the aggregation uses will be refreshed."),
    )
    return parser


if __name__ == "__main__":
    parser = get_parser()
    args = parser.parse_args()
    entity_maps = load_entity_maps_pkl()
    aggregation_class: Type[Aggregation] = aggregation_map[args.aggregation]
    aggregation = aggregation_class()
    print(f"Now running aggregation...")
    aggregation.run(entity_maps, args.verbose, args.refresh)
