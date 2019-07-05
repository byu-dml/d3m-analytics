from src.analyses.analysis import Analysis
from src.utils import has_path


class BasicStatsAnalysis(Analysis):
    def run(self, dataset: dict, verbose: bool):

        # How many runs are there?
        num_runs = len(dataset.keys())
        # What is the distribution of number of metrics per pipeline?
        metrics_cnts: dict = {}
        # What is the distribution of phase types?
        phase_cnts: dict = {}
        # What is the distribution of metric types?
        metric_types_cnt: dict = {}
        # How many pipeline runs include a normalized value with their metrics?
        num_normalized_metric_values = 0

        for run in dataset.values():

            num_scores_str = str(len(run.scores))
            if num_scores_str not in metrics_cnts:
                metrics_cnts[num_scores_str] = 0
            metrics_cnts[num_scores_str] += 1

            for score in run.scores:
                if score.metric not in metric_types_cnt:
                    metric_types_cnt[score.metric] = 0
                metric_types_cnt[score.metric] += 1
                if verbose:
                    print(
                        f"metric={score.metric}\tvalue={score.value}\tnormalized_value={score.normalized_value}"
                    )

                if score.normalized_value is not None:
                    num_normalized_metric_values += 1

            if run.run_phase not in phase_cnts:
                phase_cnts[run.run_phase] = 0
            phase_cnts[run.run_phase] += 1

        # Report
        print("\n*********************")
        print("****** RESULTS ******")
        print("*********************\n")
        print(f"The dataset has {num_runs} pipeline runs with unique ids.")
        print(f"The distribution of # of metrics per pipeline is {metrics_cnts}")
        print(f"The distribution of run phases is: {phase_cnts}")
        print(f"The distribution of metric types is: {metric_types_cnt}")
        print(
            f"There are {num_normalized_metric_values} normalized metric values found among the pipelines"
        )
