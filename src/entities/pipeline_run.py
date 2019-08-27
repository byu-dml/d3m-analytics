from typing import Union, Tuple, Dict, List
import itertools

import iso8601
import pandas as pd

from src.entities.entity import Entity, EntityWithId
from src.entities.document_reference import DocumentReference
from src.entities.pipeline import Pipeline
from src.entities.score import Score
from src.entities.problem import Problem
from src.misc.utils import has_path, enforce_field


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

        self.prediction_headers: List[str] = []
        if has_path(pipeline_run_dict, ["run", "results", "predictions", "header"]):
            for col_name in pipeline_run_dict["run"]["results"]["predictions"][
                "header"
            ]:
                self.prediction_headers.append(col_name)

        # These references will be dereferenced later by the loader
        # once the pipelines, problems, and datasets are available.
        self.datasets: list = []
        for dataset_dict in pipeline_run_dict["datasets"]:
            self.datasets.append(DocumentReference(dataset_dict))
        self.pipeline: Union[DocumentReference, Problem] = DocumentReference(
            pipeline_run_dict["pipeline"]
        )
        self.problem = DocumentReference(pipeline_run_dict["problem"])

    def post_init(self, entity_maps) -> None:
        """Dereference this pipeline run's pipeline, problem, and datasets."""
        self.pipeline = entity_maps["pipelines"][self.pipeline.digest]
        self.problem = entity_maps["problems"][self.problem.digest]
        for i, dataset_reference in enumerate(self.datasets):
            self.datasets[i] = entity_maps["datasets"][dataset_reference.digest]

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

