from typing import Tuple

import pandas as pd


class Predictions:
    def __init__(self, indices: list, values: list):
        self.data = pd.DataFrame({"indices": indices, "values": values})

    @staticmethod
    def find_common(
        preds_a: "Predictions", preds_b: "Predictions"
    ) -> Tuple[pd.Series, pd.Series]:
        """
        Get the predictions from preds_a and preds_b that are
        for the same rows in their parent pipeline's dataset.
        Returns the common prediction values aligned by dataset
        row index. So `common.values_a[i]` is a prediction for
        the same dataset row as `common.values_b[i]`.
        """
        common = pd.merge(preds_a, preds_b, on="indices", suffixes=("_a", "_b"))
        return common.values_a, common.values_b
