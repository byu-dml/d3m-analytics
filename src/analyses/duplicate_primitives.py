import itertools
import functools
from collections import namedtuple, deque, defaultdict
from typing import Dict, Tuple, List, Set, Union, Any
from math import factorial as fac
from typing import NamedTuple

from tqdm import tqdm
import matplotlib.pyplot as plt

from src.analyses.analysis import Analysis
from src.entities.pipeline import Pipeline
from src.entities.primitive import Primitive
from src.entities.pipeline_run import PipelineRun
from src.misc.utils import with_cache
from src.misc.metrics import MetricProblemType
from src.misc.settings import PredsLoadStatus


class ScoreDiff(NamedTuple):
    metric: str
    abs_score_diff: float


class PipelineRunPairDiffEntry(NamedTuple):
    run_a: PipelineRun
    run_b: PipelineRun
    output_difference: float
    output_difference_metric: MetricProblemType
    score_diffs: List[ScoreDiff] = []


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

    # Get the ids and their corresponding python paths of all
    # primitives used in the pipeline runs

    prim_id_to_paths: Dict[str, Set[str]] = defaultdict(set)
    for run in pipeline_runs.values():
        for prim_step in run.pipeline.flattened_steps:
            prim_id_to_paths[prim_step.id].add(prim_step.python_path)

    # Initialize the empty PPCM

    primitive_ids = list(prim_id_to_paths.keys())
    primitive_ids.sort()

    ppcm: Dict[Tuple[str, ...], List[PipelineRunPairDiffEntry]] = {
        primitive_pair: []
        for primitive_pair in itertools.combinations(primitive_ids, 2)
    }

    # Filter out pipeline runs invalid for this analysis
    valid_runs = [
        run
        for run in pipeline_runs.values()
        # If the run wasn't successful there may not be any
        # predictions for it; the run didn't complete.
        if run.was_successful()
        # We don't support calculating output differences for all
        # problem types yet.
        and run.problem.type in MetricProblemType.supported_types()
        # We only calculate output differences on test set
        # pipeline runs, since that measures the similarity
        # of their generalization capacity.
        and run.was_run_on_test_set()
        and run.predictions_status != PredsLoadStatus.NOT_USEABLE
    ]

    # Fill the PPCM with values

    for run_a, run_b in tqdm(
        itertools.combinations(valid_runs, 2), total=num_combinations(valid_runs, 2)
    ):
        if run_a.is_same_problem_and_context_as(run_b) and run_a.is_one_step_off_from(
            run_b
        ):
            # These two runs are one step off from each other and have
            # all other attributes needed for this analysis.

            run_a.load_predictions()
            run_b.load_predictions()

            # Get the primitive pair they are off by:
            prim_pair = run_a.pipeline.get_steps_off_from(run_b.pipeline)[0]
            prims_are_not_none = all(prim is not None for prim in prim_pair)
            prim_a, prim_b = prim_pair

            if (
                prims_are_not_none
                and prim_a.is_same_position_different_kind(prim_b)
                and run_a.predictions.same_dtype_as(run_b.predictions)
            ):
                # The two pipelines are tantamount (including each primitive's inputs
                # and outputs), but with a single pair of primitives being different.
                prim_pair = tuple(sorted(prim.id for prim in prim_pair))
                output_difference, output_difference_metric = run_a.get_output_difference_from(
                    run_b
                )
                run_diff = PipelineRunPairDiffEntry(
                    run_a, run_b, output_difference, output_difference_metric
                )
                # Next get the differences between their scores
                for metric, scores in run_a.get_scores_of_common_metrics(run_b).items():
                    run_a_score, run_b_score = scores
                    score_diff = ScoreDiff(
                        metric, abs(run_a_score.value - run_b_score.value)
                    )
                    run_diff.score_diffs.append(score_diff)
                # Add the run diff to the ppcm, mapped to the primitive pair
                # its off by.
                ppcm[prim_pair].append(run_diff)
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
                        score_diff.abs_score_diff
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

            avg_metric_diff = (
                sum(
                    score_diff.abs_score_diff
                    for diff in diff_list
                    for score_diff in diff.score_diffs
                )
                / num_diffs
            )

            avg_output_diff = (
                sum(diff.output_difference for diff in diff_list) / num_diffs
            )

            ppcl_aggregate.append(
                {
                    "prim_a_paths": prim_id_to_paths[prim_id_a],
                    "prim_b_paths": prim_id_to_paths[prim_id_b],
                    "num_diffs": num_diffs,
                    "avg_metric_diff": avg_metric_diff,
                    "avg_score_diffs_by_metric": avg_score_diffs_by_metric,
                    "avg_output_diff": avg_output_diff,
                    "avg_ouptut_diffs_by_metric": avg_output_diffs_by_metric,
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
