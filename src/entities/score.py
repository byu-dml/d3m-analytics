class Score:
    def __init__(self, score_dict):
        self.metric = score_dict["metric"]["metric"]
        self.value = score_dict["value"]
        # A normalized version of `value`.
        # Falls between 0 and 1. Higher is better.
        self.normalized_value = score_dict["normalized"]
