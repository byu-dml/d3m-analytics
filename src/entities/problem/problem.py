from typing import List

from src.entities.entity import Entity, EntityWithId
from src.misc.utils import enforce_field, has_path
from src.entities.problem.input import ProblemInput
from src.misc.settings import ProblemType


class Problem(EntityWithId):
    def __init__(self, problem_dict: dict, **kwargs):
        enforce_field(problem_dict, "digest")
        self.digest: str = problem_dict["digest"]
        self.name: str = problem_dict["name"]
        # TODO: Handle the multiple forms of problem
        # dictionaries a little more gracefully. We
        # may not be capturing as much info as we could
        # e.g. some of these `type` fields may be assigned
        # the value of `None` when they needn't be.
        self.type = self._set_problem(problem_dict)

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
        type, and inputs. Doesn't worry about the digest.
        """
        # metrics here are just strings so we can use != directly.
        if self.metrics != problem.metrics:
            return False

        if not Entity.are_lists_tantamount(self.inputs, problem.inputs):
            return False

        return self.type == problem.type

    def _set_problem(self, problem_dict: dict) -> ProblemType:
        """
        Handles the different cases for a problem dictionary,
        since the schema has changed over time.
        """
        if has_path(problem_dict, ["problem", "task_type"]):
            return ProblemType(problem_dict["problem"]["task_type"])
        elif has_path(problem_dict, ["problem", "task_keywords"]):
            # This must be a new-style problem
            possible_problems = {problem.value for problem in ProblemType}
            for keyword in problem_dict["problem"]["task_keywords"]:
                if isinstance(keyword, dict):
                    keyword = keyword["description"].replace("TaskKeyword.", "")
                if keyword in possible_problems:
                    return ProblemType(keyword)
            return None
        else:
            return None
