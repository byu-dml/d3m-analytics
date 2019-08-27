from typing import List

from src.entities.entity import Entity, EntityWithId
from src.misc.utils import enforce_field


class Problem(EntityWithId):
    def __init__(self, problem_dict: dict):
        enforce_field(problem_dict, "digest")
        self.digest = problem_dict["digest"]
        self.name = problem_dict["name"]
        self.type = problem_dict["problem"]["task_type"]
        self.subtype = problem_dict["problem"]["task_subtype"]
        self.metrics: List[str] = []
        if "performance_metrics" in problem_dict:
            for metric_dict in problem_dict["performance_metrics"]:
                self.metrics.append(metric_dict["metric"])

    def post_init(self, entity_maps) -> None:
        pass

    def get_id(self):
        return self.digest

    def is_tantamount_to(self, problem: "Problem") -> bool:
        """
        Checks to make sure the problems have the same name, metrics,
        type, and subtype. Doesn't worry about the digest.
        """
        for i, our_metric in enumerate(self.metrics):
            their_metric = problem.metrics[i]
            if our_metric != their_metric:
                return False

        return self.type == problem.type and self.subtype == problem.subtype
