from typing import List

from analytics.entities.entity import Entity
from analytics.entities.problem.target import Target


class ProblemInput(Entity):
    def __init__(self, input_dict: dict):
        self.dataset_id: str = input_dict["dataset_id"]
        self.targets: List[Target] = [
            Target(target_dict) for target_dict in input_dict["targets"]
        ]

    def is_tantamount_to(self, problem_input: "ProblemInput") -> bool:
        if self.dataset_id != problem_input.dataset_id:
            return False

        if not Entity.are_lists_tantamount(self.targets, problem_input.targets):
            return False

        return True
