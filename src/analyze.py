from argparse import ArgumentParser
from typing import Type, Mapping
import pickle

from src.analyses.analysis import Analysis
from src.analyses.basic_stats import BasicStatsAnalysis
from src.settings import DefaultDirs, DefaultFiles

analysis_map: Mapping[str, Type[Analysis]] = {"basic_stats": BasicStatsAnalysis}


def get_parser() -> ArgumentParser:
    """Configures the argument parser for analyzing a pickled extraction of pipeline runs."""
    parser = ArgumentParser(
        description="Load and analyze a pickled extraction of pipeline runs"
    )
    parser.add_argument(
        "--pkl-dir",
        "-d",
        default=DefaultDirs.EXTRACTION.value,
        help="The path to the folder that contains the pickled pipeline runs to analyze (include the folder name)",
    )
    parser.add_argument(
        "--analysis",
        "-a",
        choices=analysis_map.keys(),
        default="basic_stats",
        help="The name of the analysis to conduct",
    )
    return parser


if __name__ == "__main__":
    parser = get_parser()
    args = parser.parse_args()
    read_path = f"{args.pkl_dir}/{DefaultFiles.EXTRACTION_PKL.value}"
    print(f"Now loading pickled pipeline_runs from '{read_path}'...")
    with open(read_path, "rb") as f:
        dataset: dict = pickle.load(f)
    analysis_class: Type[Analysis] = analysis_map[args.analysis]
    analysis = analysis_class()
    print(f"Now running {args.analysis} analysis...")
    analysis.run(dataset)
