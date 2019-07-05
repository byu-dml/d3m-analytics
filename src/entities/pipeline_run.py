import iso8601

from src.entities.document_reference import DocumentReference
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

        self.dataset_digests: list = []
        for dataset_dict in pipeline_run_dict["datasets"]:
            self.dataset_digests.append(dataset_dict["digest"])

        # These references will be dereferenced later by the loader
        # once the pipelines and problems are available.
        self.pipeline = DocumentReference(pipeline_run_dict["pipeline"])
        self.problem = DocumentReference(pipeline_run_dict["problem"])

        self.run_phase = pipeline_run_dict["run"]["phase"]
        self.scores: list = []
        if has_path(pipeline_run_dict, ["run", "results", "scores"]):
            for score_dict in pipeline_run_dict["run"]["results"]["scores"]:
                self.scores.append(Score(score_dict))
