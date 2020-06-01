import itertools, uuid
from tqdm import tqdm
from collections import defaultdict
from math import factorial
from typing import Dict, List, Tuple, Set, NamedTuple

from analytics.misc.metrics import MetricProblemType
from analytics.misc.settings import PredsLoadStatus
from analytics.entities.pipeline_run import PipelineRun
from analytics.aggregations.aggregation import Aggregation


class ScoreDiff(NamedTuple):
    """
    `metric_score_diff` is calculated as the score of `run_b` (in the
    containing PipelineRunPairDiffEntry) minus the score of `run_a`. I.e.,
    a positive value indicates that `run_b` had a higher score, while a
    negative value means `run_a` had a higher score.
    """

    metric: str
    metric_score_diff: float


class PipelineRunPairDiffEntry(NamedTuple):
    run_a: PipelineRun
    run_b: PipelineRun
    output_difference: float
    output_difference_metric: MetricProblemType
    score_diffs: List[ScoreDiff]


def primitive_ids_and_paths(
    pipeline_runs: Dict[str, PipelineRun]
) -> Tuple[List[str], Dict[str, Set[str]]]:
    """
    Given a dictionary of pipeline runs, this function finds all unique primitive
    IDs and maps them to every associated python path.

    Returns
    -------
    primitive_ids : List[str]
        A sorted list of the unique primitive ids found in `pipeline_runs`.
    prim_id_to_paths : Dict[str, Set[str]]
        A mapping of each primitive id to a set of all python paths
        associated with that id, as observed in `pipeline_runs`.
    """
    prim_id_to_paths: Dict[str, Set[str]] = defaultdict(set)
    for run in pipeline_runs.values():
        for prim_step in run.pipeline.flattened_steps:
            prim_id_to_paths[prim_step.id].add(prim_step.python_path)

    primitive_ids = list(prim_id_to_paths.keys())
    primitive_ids.sort()

    return primitive_ids, prim_id_to_paths


def num_combinations(iterable, r):
    """
    Returns the number of combinations `itertools.combinations(iterable, r)`
    returns.
    Source: https://docs.python.org/2/library/itertools.html#itertools.combinations
    """
    n = len(iterable)
    return factorial(n) // factorial(r) // factorial(n - r)


class PrimitivePairComparisonAggregation(Aggregation):
    def run(
        self,
        entity_maps: Dict[str, dict],
        verbose: bool = False,
        refresh: bool = True,
        save_table: bool = True,
    ):
        """
        Builds the primitive pair comparison map (PPCM), a map of each possible
        unordered primitive pair to a list. Each pair's list contains all
        the pipeline run pairs that are almost identical, save the two primitives
        of the pair are swapped in a single position on the pipeline
        runs. In every pair of pipelines, the first pipeline is the pipeline
        containing the first primitive in the associated primitive pair key.
        As an example, in simplified syntax, the value at key
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
        prim_ids : List[str]
            A sorted list of the unique primitive ids found in `pipeline_runs`.

        Output Tables
        -------------
        primitive_pairs
            A simple table assigning an id to each pair of primitives. Referenced
            by the `diffs` table.
        diffs
            A table of the diffs between pipeline runs whose pipelines only
            differ by one primitive step. References `primitive_pairs` table
            (many-to-one). Referenced by `score_diffs` table.
        score_diffs
            A table recording by how much a pair of pipeline runs differ in a
            specific metric. References the `diffs` table (many-to-one).
        """

        pipeline_runs = entity_maps["pipeline_runs"]
        prim_ids, prim_id_to_paths = primitive_ids_and_paths(pipeline_runs)

        ppcm: Dict[Tuple[str, ...], List[PipelineRunPairDiffEntry]] = {
            primitive_pair: [] for primitive_pair in itertools.combinations(prim_ids, 2)
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

            if run_a.is_same_problem_and_context_as(
                run_b
            ) and run_a.is_one_step_off_from(run_b):
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
                    if prim_a.id != prim_pair[0]:
                        # The PPCM is indexed by prim_pair. For consistency, in each entry
                        # of the PPCM we want run_a to be the run containing the primitive
                        # that appears first in prim_pair. If the order of the primitives
                        # was switched when the tuple was sorted above, we need to swap
                        # run_a and run_b.
                        run_a, run_b = run_b, run_a

                    output_difference, output_difference_metric = run_a.get_output_difference_from(
                        run_b
                    )
                    run_diff = PipelineRunPairDiffEntry(
                        run_a, run_b, output_difference, output_difference_metric, []
                    )

                    # Next get the differences between their scores
                    for metric, scores in run_a.get_scores_of_common_metrics(
                        run_b
                    ).items():
                        run_a_score, run_b_score = scores
                        score_diff = ScoreDiff(
                            metric, run_b_score.value - run_a_score.value
                        )
                        run_diff.score_diffs.append(score_diff)
                    # Add the run diff to the ppcm, mapped to the primitive pair
                    # it's off by.
                    ppcm[prim_pair].append(run_diff)

        if save_table:
            print("Formatting data for writing to table...")

            tables = {
                "primitive_pairs": (
                    ["pair_id", "prim_a", "python_path_a", "prim_b", "python_path_b"],
                    [],
                    None,
                ),
                "diffs": (
                    [
                        "diff_id",
                        "run_a_id",
                        "run_b_id",
                        "output_diff",
                        "output_diff_metric",
                        "pair_id",
                    ],
                    [],
                    None,
                ),
                "score_diffs": (
                    ["score_diff_id", "metric", "metric_score_diff", "diff_id"],
                    [],
                    None,
                ),
            }

            keys = list(ppcm.keys())
            while len(keys) > 0:
                prim_pair = keys.pop(0)
                run_diff_list = ppcm.pop(prim_pair)
                pair_uuid = str(uuid.uuid4())

                tables["primitive_pairs"][1].append(
                    {
                        "pair_id": pair_uuid,
                        "prim_a": prim_pair[0],
                        "python_path_a": prim_id_to_paths[prim_pair[0]],
                        "prim_b": prim_pair[1],
                        "python_path_b": prim_id_to_paths[prim_pair[1]],
                    }
                )

                while len(run_diff_list) > 0:
                    run_diff = run_diff_list.pop(0)
                    diff_uuid = str(uuid.uuid4())

                    tables["diffs"][1].append(
                        {
                            "diff_id": diff_uuid,
                            "run_a_id": run_diff.run_a.get_id(),
                            "run_b_id": run_diff.run_b.get_id(),
                            "output_diff": run_diff.output_difference,
                            "output_diff_metric": run_diff.output_difference_metric.name,
                            "pair_id": pair_uuid,
                        }
                    )

                    while len(run_diff.score_diffs) > 0:
                        score_diff = run_diff.score_diffs.pop(0)
                        score_diff_uuid = str(uuid.uuid4())

                        tables["score_diffs"][1].append(
                            {
                                "score_diff_id": score_diff_uuid,
                                "metric": score_diff.metric,
                                "metric_score_diff": score_diff.metric_score_diff,
                                "diff_id": diff_uuid,
                            }
                        )

            print("Finished formatting data. Records in each table:")
            for table_name, params in tables.items():
                print(f"{table_name}: " + str(len(params[1])))

            for table_name, params in tables.items():
                self.save_table(table_name, *params)

        return {
            "ppcm": ppcm,
            "prim_id_to_paths": prim_id_to_paths,
            "prim_ids": prim_ids,
        }
