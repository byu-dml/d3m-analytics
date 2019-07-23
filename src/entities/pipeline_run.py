import iso8601
from typing import Union

from src.entities.document_reference import DocumentReference
from src.entities.pipeline import Pipeline
from src.entities.score import Score
from src.utils import has_path, enforce_field


class PipelineRun:
    def __init__(self, pipeline_run_dict: dict, should_enforce_id: bool):
        enforce_field(should_enforce_id, pipeline_run_dict, "id")
        self.id = pipeline_run_dict["id"]
        self.status = pipeline_run_dict["status"]["state"]
        self.start = iso8601.parse_date(pipeline_run_dict["start"])
        self.end = iso8601.parse_date(pipeline_run_dict["end"])
        self.submitter = pipeline_run_dict["_submitter"]

        self.run_phase = pipeline_run_dict["run"]["phase"]
        self.scores = []  # type: list
        if has_path(pipeline_run_dict, ["run", "results", "scores"]):
            for score_dict in pipeline_run_dict["run"]["results"]["scores"]:
                self.scores.append(Score(score_dict))

        # These references will be dereferenced later by the loader
        # once the pipelines, problems, and datasets are available.
        self.datasets = []  # type: list
        for dataset_dict in pipeline_run_dict["datasets"]:
            self.datasets.append(DocumentReference(dataset_dict))
        self.pipeline = DocumentReference(
            pipeline_run_dict["pipeline"]
        )  # type: Union[DocumentReference, Pipeline]
        self.problem = DocumentReference(pipeline_run_dict["problem"])

    def find_common_scores(self, run: "PipelineRun", tolerance: float = 0.0) -> list:
        """
        Returns a list of the scores `self` has that are identical to `run`'s
        scores. `tolerance` is used when assesing equality between scores.
        """
        common_scores = []
        for my_score in self.scores:
            for their_score in run.scores:
                if my_score.equals(their_score, tolerance):
                    common_scores.append(my_score)
        return common_scores

