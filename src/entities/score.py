class Score:
    def __init__(self, score_dict):
        self.metric = score_dict["metric"]["metric"]
        self.value = score_dict["value"]
        # A normalized version of `value`.
        # Falls between 0 and 1. Higher is better.
        self.normalized_value = score_dict["normalized"]

    def equals(
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
