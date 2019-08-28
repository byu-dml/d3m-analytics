from typing import Union, Tuple, Dict, List, Optional
import itertools
import json
import os

import iso8601
import pandas as pd

from src.entities.entity import Entity, EntityWithId
from src.entities.document_reference import DocumentReference
from src.entities.pipeline import Pipeline
from src.entities.score import Score
from src.entities.problem import Problem
from src.misc.utils import has_path, enforce_field
from src.misc.settings import DataDir, PredsLoadStatus


class PipelineRun(EntityWithId):
    def __init__(self, pipeline_run_dict: dict):
        enforce_field(pipeline_run_dict, "id")
        self.id = pipeline_run_dict["id"]
        self.status = pipeline_run_dict["status"]["state"]
        self.start = iso8601.parse_date(pipeline_run_dict["start"])
        self.end = iso8601.parse_date(pipeline_run_dict["end"])
        self.submitter = pipeline_run_dict["_submitter"]

        self.run_phase = pipeline_run_dict["run"]["phase"]
        self.scores: list = []
        if has_path(pipeline_run_dict, ["run", "results", "scores"]):
            for score_dict in pipeline_run_dict["run"]["results"]["scores"]:
                self.scores.append(Score(score_dict))

        # These references will be dereferenced later by the loader
        # once the pipelines, problems, and datasets are available.
        self.datasets: list = []
        for dataset_dict in pipeline_run_dict["datasets"]:
            self.datasets.append(DocumentReference(dataset_dict))
        self.pipeline: Union[DocumentReference, Problem] = DocumentReference(
            pipeline_run_dict["pipeline"]
        )
        self.problem = DocumentReference(pipeline_run_dict["problem"])

    def init_predictions(self, pipeline_run_dict) -> None:
        self.predictions_status: PredsLoadStatus = PredsLoadStatus.NOT_TRIED

        # These attributes can be added later by self.load_predictions
        # if requested
        self.prediction_indices: Optional[pd.Series[int]] = None
        self.predictions: Optional[
            Union[pd.Series[float], pd.Series[int], pd.Series[str]]
        ] = None

        # Initialize the prediction headers.
        self.prediction_headers: List[str] = []
        if not has_path(pipeline_run_dict, ["run", "results", "predictions", "header"]):
            # Without prediction column headers, we won't know which
            # column of predictions is which.
            self.predictions_status = PredsLoadStatus.NOT_USEABLE
            return

        for col_name in pipeline_run_dict["run"]["results"]["predictions"]["header"]:
            self.prediction_headers.append(col_name)

        if "d3mIndex" not in self.prediction_headers:
            # Without the `d3mIndex` column we won't know which
            # prediction goes with which instanct of a dataset.
            self.predictions_status = PredsLoadStatus.NOT_USEABLE

    def post_init(self, entity_maps) -> None:
        """Dereference this pipeline run's pipeline, problem, and datasets."""
        self.pipeline = entity_maps["pipelines"][self.pipeline.digest]
        self.problem = entity_maps["problems"][self.problem.digest]
        for i, dataset_reference in enumerate(self.datasets):
            self.datasets[i] = entity_maps["datasets"][dataset_reference.digest]

    def load_predictions(self) -> None:
        """
        Loads the predictions for this pipeline run identified by
        its id. Loads them from the DB dump.
        """
        # First, pass all the checks required to determine if this run
        # has predictions we can use. There is some irregularity in the
        # predictions reported in the pipeline runs. We check here for
        # the most popular formatting, and just use predictions for runs
        # that follow that formating scheme i.e. two columns, with one
        # of them being called "d3mIndex", holding the row numbers of the
        # dataset instances predicted on.

        if self.predictions_status != PredsLoadStatus.NOT_TRIED:
            # Either the predictions have already been loaded, or
            # they've been determined not useable.
            return

        path_to_preds = os.path.join(DataDir.PREDICTIONS_DUMP.value, f"{self.id}.json")
        with open(path_to_preds, "r") as f:
            pipeline_run_preds_dict = json.load(f)

        if not has_path(pipeline_run_preds_dict, ["run", "results", "predictions"]):
            self.predictions_status = PredsLoadStatus.NOT_USEABLE
            return

        predictions_dict = pipeline_run_preds_dict["run"]["results"]["predictions"]

        if "values" not in predictions_dict:
            self.predictions_status = PredsLoadStatus.NOT_USEABLE
            return

        if len(predictions_dict["values"]) != 2:
            self.predictions_status = PredsLoadStatus.NOT_USEABLE
            return

        # All checks passed. This run has predictions we can use.

        i_of_prediction_indices = self.prediction_headers.index("d3mIndex")
        self.prediction_indices = pd.Series(
            predictions_dict["values"][i_of_prediction_indices]
        )

        # The predictions are just in the column that the prediction indices aren't,
        i_of_predictions = 0 if i_of_prediction_indices == 1 else 1
        self.predictions = pd.Series(predictions_dict["values"][i_of_predictions])

        self.predictions_status = PredsLoadStatus.USEABLE

    def get_id(self):
        return self.id

    def is_tantamount_to(self, run: "PipelineRun") -> bool:
        raise NotImplementedError

    def find_common_scores(self, run: "PipelineRun", tolerance: float = 0.0) -> list:
        """
        Returns a list of the scores `self` has that are identical to `run`'s
        scores. `tolerance` is used when assesing equality between scores.
        """
        common_scores = []
        for my_score in self.scores:
            for their_score in run.scores:
                if my_score.is_tantamount_to_with_tolerance(their_score, tolerance):
                    common_scores.append(my_score)
        return common_scores

    def is_same_problem_and_context_as(self, run: "PipelineRun") -> bool:
        for our_dataset, their_dataset in zip(self.datasets, run.datasets):
            if not our_dataset.is_tantamount_to(their_dataset):
                return False

        if self.run_phase != run.run_phase:
            return False

        if self.status != run.status:
            return False

        if not self.problem.is_tantamount_to(run.problem):
            return False

        return True

    def is_one_step_off_from(self, run: "PipelineRun") -> bool:
        """
        Checks to see if `self` is one step off from `run`.
        They are one step off if there is only a single primitive
        that is different between the two.
        """
        return self.pipeline.get_num_steps_off_from(run.pipeline) == 1

    def get_scores_of_common_metrics(
        self, run: "PipelineRun"
    ) -> Dict[str, Tuple[Score, Score]]:
        """
        Finds the scores for the metrics common between `self` and `run`.
        Returns a dict mapping the metric name to the tuple of the runs'
        scores for that metric.
        """
        common_metrics: Dict[str, Tuple[Score, Score]] = {}
        for our_score in self.scores:
            for their_score in run.scores:
                if our_score.metric == their_score.metric:
                    common_metrics[our_score.metric] = (our_score, their_score)

        return common_metrics

