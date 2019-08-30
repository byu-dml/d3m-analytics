from typing import List

from src.entities.entity import Entity, EntityWithId
from src.misc.utils import enforce_field
from src.entities.problem_input import ProblemInput
from src.misc.settings import ProblemType


class Problem(EntityWithId):
    def __init__(self, problem_dict: dict):
        enforce_field(problem_dict, "digest")
        self.digest: str = problem_dict["digest"]
        self.name: str = problem_dict["name"]
        self.type: ProblemType = ProblemType(problem_dict["problem"]["task_type"])
        self.subtype: str = problem_dict["problem"]["task_subtype"]

        self.metrics: List[str] = []
        if "performance_metrics" in problem_dict:
            for metric_dict in problem_dict["performance_metrics"]:
                self.metrics.append(metric_dict["metric"])
        self.metrics.sort()

        self.inputs: List[ProblemInput] = []
        for input_dict in problem_dict["inputs"]:
            self.inputs.append(ProblemInput(input_dict))

    def post_init(self, entity_maps) -> None:
        pass

    def get_id(self):
        return self.digest

    def is_tantamount_to(self, problem: "Problem") -> bool:
        """
        Checks to make sure the problems have the same name, metrics,
        type, subtype, and inputs. Doesn't worry about the digest.
        """
        if len(self.metrics) != len(problem.metrics):
            return False

        for our_metric, their_metric in zip(self.metrics, problem.metrics):
            if our_metric != their_metric:
                return False

        if len(self.inputs) != len(problem.inputs):
            return False

        for our_input, their_input in zip(self.inputs, problem.inputs):
            if not our_input.is_tantamount_to(their_input):
                return False

        return self.type == problem.type and self.subtype == problem.subtype
