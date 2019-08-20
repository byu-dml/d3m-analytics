import itertools
import functools
from collections import namedtuple, deque
from typing import Dict, Tuple, List, Set, Union, Any
from math import factorial as fac

from tqdm import tqdm
import matplotlib.pyplot as plt

from src.analyses.analysis import Analysis
from src.entities.pipeline import Pipeline
from src.entities.primitive import Primitive
from src.entities.pipeline_run import PipelineRun
from src.misc.utils import set_default, with_cache


PipelineRunPairDiffEntry = namedtuple(
    "PipelineRunPairDiffEntry", ["run_a", "run_b", "metric", "abs_score_diff"]
)


def num_combinations(iterable, r):
    """
    Returns the number of combinations `itertools.combinations(iterable, r)`
    returns.
    Source: https://docs.python.org/2/library/itertools.html#itertools.combinations
    """
    n = len(iterable)
    return fac(n) // fac(r) // fac(n - r)


def build_ppcm(pipeline_runs: Dict[str, PipelineRun]):
    """
    Builds the primitive pair comparison map (PPCM), a map of each possible
    unordered primitive pair to a list. Each pair's list contains all
    the pipeline run pairs that are almost identical, save the two primitives
    of the pair are swapped in a single position on the pipeline
    runs. As an example, in simplified syntax, the value at key
    PPCM[("randomforest", "gradientboosting")] might have a list that contains the
    pipeline pair: [dataset_to_dataframe, imputer, randomforest,
    construct_predictions], [dataset_to_dataframe, imputer, gradientboosting,
    construct_predictions], since these pipeline runs are only 1 primitive off
    from each other, and the two primitives they're off on is randomforest
    and gradientboosting.

    Returns
    -------
    ppcm : Dict[Tuple[str, str], List[PipelineRunPairDiffEntry]]
        The primitive pair comparison map (PPCM)
    prim_id_to_paths : Dict[str, Set[str]]
        A mapping of each primitive id to a set of all python paths
        associated with that id, as observed in `pipeline_runs`.
    primitive_ids : List[str]
        A sorted list of the unique primitive ids found in `pipeline_runs`. 
    """
    # Get the ids and python paths of all primitives used in the pipeline runs
    prim_id_to_paths: Dict[str, Set[str]] = {}
    for run in pipeline_runs.values():
        for prim_step in run.pipeline.flattened_steps:
            set_default(prim_id_to_paths, prim_step.id, set())
            prim_id_to_paths[prim_step.id].add(prim_step.python_path)

    primitive_ids = list(prim_id_to_paths.keys())
    primitive_ids.sort()

    ppcm: Dict[Tuple[str, ...], List[PipelineRunPairDiffEntry]] = {
        primitive_pair: []
        for primitive_pair in itertools.combinations(primitive_ids, 2)
    }

    # Fill the PPCM with values

    for run_a, run_b in tqdm(
        itertools.combinations(pipeline_runs.values(), 2),
        total=num_combinations(pipeline_runs.values(), 2),
    ):
        if run_a.is_same_problem_and_context_as(run_b) and run_a.is_one_step_off_from(
            run_b
        ):
            # These two runs are one step off from each other. Get the primitive pair
            # they are off by:
            pair = run_a.pipeline.get_steps_off_from(run_b.pipeline)[0]
            prims_are_not_none = all(prim is not None for prim in pair)
            prim_a, prim_b = pair

            if prims_are_not_none and prim_a.is_same_position_different_kind(prim_b):
                # The two pipelines are tantamount (including each primitive's inputs
                # and outputs), but with a single pair of primitives being different.
                pair = tuple(sorted(prim.id for prim in pair))
                # Next get the differences between their scores
                for metric, scores in run_a.get_scores_of_common_metrics(run_b).items():
                    run_a_score, run_b_score = scores
                    ppcm[pair].append(
                        PipelineRunPairDiffEntry(
                            run_a,
                            run_b,
                            metric,
                            abs(run_a_score.value - run_b_score.value),
                        )
                    )
    return ppcm, prim_id_to_paths, primitive_ids


class DuplicatePrimitivesAnalysis(Analysis):
    """
    An analysis that answers the question: Which models/preprocessor
    primitives do essentially the same thing? (I.e. which ones give
    the exact same results when used interchangeably?
    """

    def run(self, entity_maps: Dict[str, dict], verbose: bool, refresh: bool):

        # config
        num_top_pairs_to_show = 10

        pipeline_runs = entity_maps["pipeline_runs"]
        ppcm, prim_id_to_paths, primitive_ids = with_cache(build_ppcm, refresh)(
            pipeline_runs
        )
        # A list version of the PPCM
        ppcl: Any = [(prim_pair, diff_list) for prim_pair, diff_list in ppcm.items()]
        ppcl.sort(key=lambda entry: len(entry[1]), reverse=True)

        # Get the distribution of entries in the PPCM.
        ppcm_distribution = [len(diff_list) for _, diff_list in ppcl]
        num_ppcm_entries = sum(ppcm_distribution)

        # Aggregate information about the primitive pairs, so we can see
        # how many diffs each pair has, what the average score difference
        ppcl_aggregate = []
        for (prim_id_a, prim_id_b), diff_list in ppcl[:num_top_pairs_to_show]:
            num_diffs = len(diff_list)

            score_diffs_by_metric: Dict[str, List[float]] = {}
            for diff in diff_list:
                set_default(score_diffs_by_metric, diff.metric, [])
                score_diffs_by_metric[diff.metric].append(diff.abs_score_diff)

            avg_score_diffs_by_metric: Dict[str, Dict[str, float]] = {}
            for metric, score_diffs in score_diffs_by_metric.items():
                num_score_diffs = len(score_diffs)
                avg_score_diffs_by_metric[metric] = {
                    "avg": sum(score_diffs) / num_score_diffs,
                    "count": num_score_diffs,
                }

            avg_metric_diff = sum(diff.abs_score_diff for diff in diff_list) / num_diffs

            ppcl_aggregate.append(
                {
                    "prim_a_paths": prim_id_to_paths[prim_id_a],
                    "prim_b_paths": prim_id_to_paths[prim_id_b],
                    "num_diffs": num_diffs,
                    "avg_metric_diff": avg_metric_diff,
                    "avg_score_diffs_by_metric": avg_score_diffs_by_metric,
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
