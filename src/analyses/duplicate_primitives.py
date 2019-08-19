import itertools
import functools
from collections import namedtuple
from typing import Dict, Tuple, List, Set, Union, Any
from math import factorial as fac

from tqdm import tqdm

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


def build_ppcl(pipeline_runs: Dict[str, PipelineRun]):
    """
    Builds the primitive pair comparison list (PPCL), a map of each possible
    unordered primitive pair to a list. Each pair's list contains all
    the pipeline run pairs that are almost identical, save the two primitives
    of the pair are swapped in a single position on the pipeline
    runs. As an example, in simplified syntax, the value at key
    PPCL[("randomforest", "gradientboosting")] might have a list that contains the
    pipeline pair: [dataset_to_dataframe, imputer, randomforest,
    construct_predictions], [dataset_to_dataframe, imputer, gradientboosting,
    construct_predictions], since these pipeline runs are only 1 primitive off
    from each other, and the two primitives they're off on is randomforest
    and gradientboosting.

    Returns
    -------
    ppcl : Dict[Tuple[str, str], List[PipelineRunPairDiffEntry]]
        The primitive pair comparison list (PPCL)
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

    ppcl: Dict[Tuple[str, ...], List[PipelineRunPairDiffEntry]] = {
        primitive_pair: []
        for primitive_pair in itertools.combinations(primitive_ids, 2)
    }

    # Fill the PPCL with values

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
                    try:
                        ppcl[pair].append(
                            PipelineRunPairDiffEntry(
                                run_a,
                                run_b,
                                metric,
                                abs(run_a_score.value - run_b_score.value),
                            )
                        )
                    except KeyError as e:
                        print("\nrun_a:")
                        print(run_a.pipeline.print_steps())
                        print("\nrun_b")
                        print(run_b.pipeline.print_steps())
                        print(
                            f"run_a.is_one_step_off_from(run_b): {run_a.is_one_step_off_from(run_b)}"
                        )
                        print(
                            f"run_b.is_one_step_off_from(run_a): {run_b.is_one_step_off_from(run_a)}"
                        )
                        print(
                            f"run_a.pipeline.get_steps_off_from(run_b.pipeline): {run_a.pipeline.get_steps_off_from(run_b.pipeline)}"
                        )
                        print(
                            f"run_b.pipeline.get_steps_off_from(run_a.pipeline): {run_b.pipeline.get_steps_off_from(run_a.pipeline)}"
                        )
                        raise e
    return ppcl, prim_id_to_paths, primitive_ids


class DuplicatePrimitivesAnalysis(Analysis):
    """
    An analysis that answers the question: Which models/preprocessor
    primitives do essentially the same thing? (I.e. which ones give
    the exact same results when used interchangeably?
    """

    def run(self, entity_maps: Dict[str, dict], verbose: bool, refresh: bool):

        pipeline_runs = entity_maps["pipeline_runs"]
        ppcl, prim_id_to_paths, primitive_ids = with_cache(build_ppcl, refresh)(
            pipeline_runs
        )

        # Get number of entries in the PPCL; the number of pipeline pairs that are 1-off
        # from each other.

        num_ppcl_entries = functools.reduce(
            lambda acc, key: acc + len(ppcl[key]), ppcl, 0
        )

        # Report

        print("\n*********************")
        print("****** RESULTS ******")
        print("*********************\n")

        print(
            f"There are {len(primitive_ids)} distinct primitives used in the pipeline runs"
        )

        print(
            f"There are {num_ppcl_entries} pipeline run pairs that are just one primitive off"
        )

