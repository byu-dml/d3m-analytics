import itertools
import functools
from collections import namedtuple, deque, defaultdict
from typing import Dict, Tuple, List, Set, Union, Any
from math import factorial as fac
from typing import NamedTuple

from tqdm import tqdm
import matplotlib.pyplot as plt

from src.analyses.analysis import Analysis
from src.aggregations.primitive_pairs import PrimitivePairComparisonAggregation, ScoreDiff, PipelineRunPairDiffEntry
from src.entities.pipeline import Pipeline
from src.entities.primitive import Primitive
from src.entities.pipeline_run import PipelineRun
from src.misc.utils import with_cache


class DuplicatePrimitivesAnalysis(Analysis):
    """
    An analysis that answers the question: Which models/preprocessor
    primitives do essentially the same thing? (I.e. which ones give
    the exact same results when used interchangeably?
    """

    required_aggregations = [PrimitivePairComparisonAggregation]

    def run(self, entity_maps: Dict[str, dict], verbose: bool, refresh: bool,
            aggregations: Dict[str, Any]=None):

        # config
        num_top_pairs_to_show = 10

        pipeline_runs = entity_maps["pipeline_runs"]
        prim_id_to_paths = aggregations['PrimitivePairComparisonAggregation']['prim_id_to_paths']
        primitive_ids = aggregations['PrimitivePairComparisonAggregation']['prim_ids']

        # A list version of the PPCM
        ppcl: Any = [
            (prim_pair, diff_list)
            for prim_pair, diff_list
            in aggregations['PrimitivePairComparisonAggregation']['ppcm'].items()
        ]
        ppcl.sort(key=lambda entry: len(entry[1]), reverse=True)

        # Get the distribution of entries in the PPCM.
        ppcm_distribution = [len(diff_list) for _, diff_list in ppcl]
        num_ppcm_entries = sum(ppcm_distribution)

        # Aggregate information about the primitive pairs, so we can see
        # how many diffs each pair has, what the average score difference is, etc.
        ppcl_aggregate = []
        for (prim_id_a, prim_id_b), diff_list in ppcl[:num_top_pairs_to_show]:
            num_diffs = len(diff_list)

            score_diffs_by_metric: Dict[str, List[float]] = defaultdict(list)
            output_diffs_by_metric: Dict[str, List[float]] = defaultdict(list)
            for diff in diff_list:
                output_diffs_by_metric[diff.output_difference_metric.name].append(
                    diff.output_difference
                )
                for score_diff in diff.score_diffs:
                    score_diffs_by_metric[score_diff.metric].append(
                        score_diff.metric_score_diff
                    )

            avg_score_diffs_by_metric: Dict[str, Dict[str, float]] = {}
            for metric, score_diffs in score_diffs_by_metric.items():
                num_score_diffs = len(score_diffs)
                avg_score_diffs_by_metric[metric] = {
                    "avg": sum(score_diffs) / num_score_diffs,
                    "count": num_score_diffs,
                }

            avg_output_diffs_by_metric: Dict[str, Dict[str, float]] = {}
            for metric_name, output_diffs in output_diffs_by_metric.items():
                num_output_diffs = len(output_diffs)
                avg_output_diffs_by_metric[metric_name] = {
                    "avg": sum(output_diffs) / num_output_diffs,
                    "count": num_output_diffs,
                }

            avg_output_diff = (
                sum(diff.output_difference for diff in diff_list) / num_diffs
            )

            ppcl_aggregate.append(
                {
                    "prim_a_paths": prim_id_to_paths[prim_id_a],
                    "prim_b_paths": prim_id_to_paths[prim_id_b],
                    "num_diffs": num_diffs,
                    "avg_score_diffs_by_metric": avg_score_diffs_by_metric,
                    "avg_output_diff": avg_output_diff,
                    "avg_output_diffs_by_metric": avg_output_diffs_by_metric,
                }
            )

        # Report

        print("\n*********************")
        print("****** RESULTS ******")
        print("*********************")

        print(
            f"\nThere are {len(primitive_ids)} distinct primitives used in the pipeline runs"
        )

        print(
            f"\nThere are {num_ppcm_entries} pipeline run pairs that are just one primitive off"
        )
        print(
            f"\nThe {num_top_pairs_to_show} primitve pairs causing the most one-offs on pipeline runs are:"
        )
        self.pp(ppcl_aggregate[:num_top_pairs_to_show])

        if verbose:
            plt.hist(ppcm_distribution, bins=100, log=True)
            plt.xlabel("Number of Diffs For Primitive Pair")
            plt.ylabel("Count")
            plt.title(
                f"Distribution of Diff Counts For Primitive Pairs Across Pipeline Runs"
            )
            plt.show()
