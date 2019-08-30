from typing import List

from src.entities.entity import Entity
from src.entities.target import Target


class ProblemInput(Entity):
    def __init__(self, input_dict: dict):
        self.dataset_id: str = input_dict["dataset_id"]
        self.targets: List[Target] = [
            Target(target_dict) for target_dict in input_dict["targets"]
        ]

    def is_tantamount_to(self, problem_input: "ProblemInput") -> bool:
        if self.dataset_id != problem_input.dataset_id:
            return False

        if len(self.targets) != len(problem_input.targets):
            return False

        for my_target, their_target in zip(self.targets, problem_input.targets):
            if not my_target.is_tantamount_to(their_target):
                return False

        return True
