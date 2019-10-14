from argparse import ArgumentParser
from typing import Type, Mapping, Dict, List, Any

from src.misc.utils import load_entity_maps_pkl
from src.analyses.analysis import Analysis
from src.analyses.basic_stats import BasicStatsAnalysis
from src.analyses.duplicate_pipelines import DuplicatePipelinesAnalysis
from src.analyses.duplicate_primitives import DuplicatePrimitivesAnalysis

analysis_map: Mapping[str, Type[Analysis]] = {
    "basic_stats": BasicStatsAnalysis,
    "duplicate_pipelines": DuplicatePipelinesAnalysis,
    "duplicate_primitives": DuplicatePrimitivesAnalysis,
}


def get_parser() -> ArgumentParser:
    """
    Configures the argument parser for analyzing a pickled extraction of pipeline runs.
    """
    parser = ArgumentParser(
        description="Load and analyze a pickled extraction of pipeline runs"
    )
    parser.add_argument(
        "--analysis",
        "-a",
        choices=analysis_map.keys(),
        default="basic_stats",
        help="The name of the analysis to conduct",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help=(
            "If present, the results of the analysis will be reported more "
            "verbosely, if the analysis has verbose per-pipeline results "
            "to report that is"
        ),
    )
    parser.add_argument(
        "--refresh",
        "-r",
        action="store_true",
        help=("If present, any caches that the analysis uses will be refreshed."),
    )
    return parser


def load_aggregations(req_aggs: List[str]) -> Dict[str, Any]:
    aggs = {}
    if len(req_aggs) == 0:
        return aggs

    for agg_class in req_aggs:
        agg_name = agg_class.__name__
        agg = agg_class()
        print(f'Running aggregation {agg_name}...')
        aggs[agg_name] = agg.run(entity_maps=entity_maps, save_table=False)

    return aggs


if __name__ == "__main__":
    parser = get_parser()
    args = parser.parse_args()
    entity_maps = load_entity_maps_pkl()
    analysis_class: Type[Analysis] = analysis_map[args.analysis]
    analysis = analysis_class()
    aggregations = load_aggregations(analysis.required_aggregations)

    print(f"Now running {args.analysis} analysis...")
    analysis.run(entity_maps=entity_maps, aggregations=aggregations, verbose=args.verbose, refresh=args.refresh)
