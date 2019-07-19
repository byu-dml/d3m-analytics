from typing import Mapping
import itertools

from src.analyses.analysis import Analysis
from src.entities.pipeline import Pipeline
from src.entities.primitive import Primitive
from src.utils import set_default


class DuplicatePipelinesAnalysis(Analysis):
    """
    An analysis that answers the question: "Which pipelines
    achieved the exact same performance for a given dataset,
    or across all datasets?"
    """

    def run(self, pipeline_runs: dict, verbose: bool):

        # config
        run_score_comparison_tolerance = 0.001
        num_duplicate_runs_by_dataset = 10

        # First, organize all pipeline runs by dataset.
        # Only include pipeline runs that have scores
        # and don't include runs whose pipelines have the
        # same steps as previous pipeline runs.

        runs_by_dataset: dict = {}
        for run in pipeline_runs.values():

            if len(run.scores) > 0:
                for run_dataset in run.datasets:
                    set_default(runs_by_dataset, run_dataset, [])
                    pipeline_run_has_same_steps_as_previous = [
                        run.pipeline.has_same_steps(prev_run.pipeline)
                        for prev_run in runs_by_dataset[run_dataset]
                    ]
                    if not any(pipeline_run_has_same_steps_as_previous):
                        runs_by_dataset[run_dataset].append(run)

        # Next, find all pairs of pipeline runs that have the same scores
        # for a given datset.

        same_runs_by_dataset: dict = {}
        for dataset, dataset_runs in runs_by_dataset.items():
            same_runs_by_dataset[dataset] = []

            for run_a, run_b in itertools.combinations(dataset_runs, 2):
                common_scores = run_a.find_common_scores(
                    run_b, run_score_comparison_tolerance
                )
                if len(common_scores) > 0:
                    same_runs_by_dataset[dataset].append((run_a, run_b))

        same_run_counts_by_dataset = [
            (dataset, len(runs)) for dataset, runs in same_runs_by_dataset.items()
        ]
        same_run_counts_by_dataset = sorted(
            same_run_counts_by_dataset, key=lambda toop: toop[1], reverse=True
        )
        dataset_with_most_duplicate_runs = same_run_counts_by_dataset[0][0]

        # Report

        print("\n*********************")
        print("****** RESULTS ******")
        print("*********************\n")

        print(
            f"The {num_duplicate_runs_by_dataset} dataset(s) with the most runs having the same scores (considered with a tolerance of {run_score_comparison_tolerance}) are:"
        )
        for dataset, count in same_run_counts_by_dataset[
            :num_duplicate_runs_by_dataset
        ]:
            print(f"\t{dataset.name}\t{count}")
        print(
            f"The steps of the {num_duplicate_runs_by_dataset} run pair(s) with the same score, for the dataset with the most 'duplicate' runs are:"
        )
        for i, run_pair in enumerate(
            same_runs_by_dataset[dataset_with_most_duplicate_runs][
                :num_duplicate_runs_by_dataset
            ]
        ):
            print(f"Pair {i+1}:")
            print(
                f"run A (score {[(score.metric,score.value) for score in run_pair[0].scores]}):"
            )
            run_pair[0].pipeline.print_steps(use_short_path=True, indent=1)
            print(
                f"run B (score {[(score.metric,score.value) for score in run_pair[1].scores]}):"
            )
            run_pair[1].pipeline.print_steps(use_short_path=True, indent=1)
