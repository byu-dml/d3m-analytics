from typing import Dict, Any
from collections import defaultdict

import matplotlib.pyplot as plt
import matplotlib.dates as mdates

from src.analyses.analysis import Analysis
from src.entities.pipeline import Pipeline
from src.entities.primitive import Primitive


class BasicStatsAnalysis(Analysis):
    """
    An analysis that considers basic statistics and features of the
    pipeline run data.
    """

    def run(
        self,
        entity_maps: Dict[str, dict],
        verbose: bool,
        refresh: bool,
        aggregations: Dict[str, Any] = None,
    ):
        pipeline_runs = entity_maps["pipeline_runs"]

        # config
        num_top_primitives = 40

        # How many runs are there?
        num_runs = len(pipeline_runs.keys())
        # What is the distribution of pipeline run scores by metric type?
        metric_values: dict = defaultdict(list)
        # What is the distribution of phase types?
        phase_cnts: dict = defaultdict(int)
        # What is the distribution of problem types?
        problem_type_cnts: dict = defaultdict(int)
        # What is the distribution of metric types?
        metric_types_cnt: dict = defaultdict(int)
        # What is the distribution of number of datasets per run?
        num_datasets_cnt: dict = defaultdict(int)
        # What is the distribution of dataset digests used among runs?
        dataset_digests_cnt: dict = defaultdict(int)
        # What is the distribution of number of scores per run?
        score_cnt: dict = defaultdict(int)
        # How many pipeline runs include a normalized value with their metrics?
        num_normalized_metric_values = 0
        # Which primitives are most common?
        primitives_cnt: dict = defaultdict(int)
        # How many sub-pipelines are being used?
        num_subpipelines = 0
        # What is the distribution of pipeline authors among pipeline runs?
        author_values: dict = defaultdict(int)
        # What is the distribution of prediction column header count among runs?
        pred_header_cnt: dict = defaultdict(int)
        # What is the distribution of prediction column header names among runs?
        pred_header_names_cnt: dict = defaultdict(int)
        # What is the distribution of number of inputs pipeline run
        # problems are taking?
        problem_input_cnt: dict = defaultdict(int)
        # What is the distribution of number of targets each run is predicting?
        targets_cnt: dict = defaultdict(int)
        # When were pipeline runs run?
        end_times: list = []
        # What is the distribution across run submitters?
        submitter_cnt: dict = defaultdict(int)

        for run in pipeline_runs.values():

            for step in run.pipeline.steps:
                if isinstance(step, Primitive):
                    primitives_cnt[step.python_path] += 1
                elif isinstance(step, Pipeline):
                    num_subpipelines += 1

            num_scores = len(run.scores)
            score_cnt[num_scores] += 1

            for score in run.scores:
                metric_values[score.metric].append(score.value)
                metric_types_cnt[score.metric] += 1
                if verbose:
                    print(
                        (
                            f"metric={score.metric}\tvalue={score.value}\t"
                            f"normalized_value={score.normalized_value}"
                        )
                    )

                if score.normalized_value is not None:
                    num_normalized_metric_values += 1

            num_datasets_cnt[len(run.datasets)] += 1

            for dataset in run.datasets:
                dataset_digests_cnt[dataset.digest] += 1

            phase_cnts[run.run_phase] += 1

            problem_type_cnts[run.problem.type] += 1

            author_values[run.pipeline.source_name] += 1

            pred_header_cnt[len(run.prediction_headers)] += 1
            for col_name in run.prediction_headers:
                pred_header_names_cnt[col_name] += 1

            problem_input_cnt[len(run.problem.inputs)] += 1

            targets_cnt[
                sum(len(problem_input.targets) for problem_input in run.problem.inputs)
            ] += 1

            end_times.append(mdates.date2num(run.end))

            submitter_cnt[run.submitter] += 1
        # Sort the primitive counts to get the most common
        primitives_cnt_tuples: Any = primitives_cnt.items()

        def get_count(toop):
            return toop[1]

        primitives_cnt_tuples = sorted(
            primitives_cnt_tuples, key=get_count, reverse=True
        )

        # Report

        print("\n*********************")
        print("****** RESULTS ******")
        print("*********************\n")

        if verbose:
            for metric_type, metric_values in metric_values.items():
                plt.hist(metric_values, bins=20)
                plt.xlabel("score")
                plt.ylabel("count")
                plt.title(
                    (
                        f"Distribution of {metric_type} Scores Across Pipeline "
                        f"Runs ({len(metric_values)} runs)"
                    )
                )
                plt.show()

        if verbose:
            # Give the distribution of pipeline end times
            # Source:
            # https://stackoverflow.com/questions/29672375/histogram-in-matplotlib-time-on-x-axis

            fig, ax = plt.subplots(1, 1)
            ax.hist(end_times, bins=50, color="cornflowerblue")
            ax.xaxis.set_major_locator(mdates.AutoDateLocator())
            ax.xaxis.set_major_formatter(mdates.DateFormatter("%m/%d/%y"))
            plt.setp(ax.xaxis.get_majorticklabels(), rotation=30)
            plt.xlabel("pipeline run end time")
            plt.ylabel("count")
            plt.title("Distribution of Pipeline Run End Times")
            plt.show()

        print(f"\nThe number of pipeline runs with unique ids is: {num_runs}")
        print(f"\nThe distribution of run phases is: {phase_cnts}")
        print(f"\nThe distribution of problem types is: {problem_type_cnts}")
        print(f"\nThe distribution of metric types is: {metric_types_cnt}")
        print(f"\nThe distribution of score counts per run is: {score_cnt}")
        print(
            f"\nThe distribution of problem inputs across runs is: {problem_input_cnt}"
        )
        print(f"\nThe distribution of target counts across runs is: {targets_cnt}")

        print(
            f"\nThe distribution of number of datasets per run is: {num_datasets_cnt}"
        )
        print(
            f"\nThe number of distinct dataset found among runs is: {len(dataset_digests_cnt)}"
        )
        print(f"\nThe distribution of submitters among runs is {submitter_cnt}")

        if verbose:
            plt.hist(dataset_digests_cnt.values())
            plt.xlabel("dataset frequency")
            plt.ylabel("count")
            plt.title("Distribution of dataset frequency among runs")
            plt.show()

        print(f"\nThe distribution of pipeline authors among runs is: {author_values}")
        print(
            f"\nThe distribution of prediction column header count is: {pred_header_cnt}"
        )
        print(
            f"\nThe distribution of prediction column header names is: {pred_header_names_cnt}"
        )
        print(
            f"\nThe number of normalized metric values found among "
            f"pipelines is: {num_normalized_metric_values}"
        )
        print(f"\nThe number of sub-pipelines found among runs is: {num_subpipelines}")
        print(f"\nThe {num_top_primitives} most commonly used primitives are:")
        for prim_cnt in primitives_cnt_tuples[
            : min(num_top_primitives, len(primitives_cnt_tuples))
        ]:
            print(f"\t{prim_cnt[0]}\t{prim_cnt[1]}")
