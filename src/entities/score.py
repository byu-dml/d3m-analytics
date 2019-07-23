from src.entities.entity import Entity


class Score(Entity):
    def __init__(self, score_dict):
        self.metric = score_dict["metric"]["metric"]
        self.value = score_dict["value"]
        # A normalized version of `value`.
        # Falls between 0 and 1. Higher is better.
        self.normalized_value = score_dict["normalized"]

    def is_tantamount_to(self, score: "Score") -> bool:
        """
        With this class, if `is_tantamount_to` returns `True`, `self`
        and `score` are fully equivalent.
        """
        return (
            self.metric == score.metric
            and self.value == score.value
            and self.normalized_value == score.normalized_value
        )

    def is_tantamount_to_with_tolerance(
        self, score: "Score", tolerance: float = 0.0, use_normalized_value: bool = False
    ) -> bool:
        """
        Returns `True` if `self` and `score` are the same by metric type and value,
        within a given `tolerance` when considering `value`.
        """
        if use_normalized_value:
            diff = abs(self.normalized_value - score.normalized_value)
        else:
            diff = abs(self.value - score.value)

        if self.metric == score.metric and diff <= tolerance:
            return True
        else:
            return False
