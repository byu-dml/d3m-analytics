from src.utils import enforce_field


class Problem:
    def __init__(self, problem_dict: dict, should_enforce_id: bool):
        enforce_field(should_enforce_id, problem_dict, "digest")
        self.digest = problem_dict["digest"]
        self.name = problem_dict["name"]
        self.type = problem_dict["problem"]["task_type"]
        self.subtype = problem_dict["problem"]["task_subtype"]
        self.metrics = []  # type: list
        if "performance_metrics" in problem_dict:
            for metric_dict in problem_dict["performance_metrics"]:
                self.metrics.append(metric_dict["metric"])
