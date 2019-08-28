from enum import Enum
from typing import Dict, Callable

import pandas as pd
import numpy as np


class MetricProblemType(Enum):
    COD = ["CLASSIFICATION", "SEMISUPERVISED_CLASSIFICATION", "VERTEX_CLASSIFICATION"]
    # ROD is "Regressor Output Difference"
    ROD = ["REGRESSION", "TIME_SERIES_FORECASTING"]

    @classmethod
    def all_supported_types(cls):
        # cls here is the enumeration
        return [problem_type for metric in cls for problem_type in metric.value]


def calculate_cod(preds_a: pd.Series, preds_b: pd.Series):
    """
    COD is the Classifier Output Difference.
    """
    differences = preds_a != preds_b
    return differences.sum() / differences.size


def calculate_rod(preds_a: pd.Series, preds_b: pd.Series):
    """
    Regressor Output Difference (ROD) is just the euclidian
    distance between the two series of predictions.
    """
    assert preds_a.size == preds_b.size
    return np.linalg.norm(preds_a - preds_b)


metric_to_f: Dict[MetricProblemType, Callable] = {
    MetricProblemType.COD: calculate_cod,
    MetricProblemType.ROD: calculate_rod,
}


def calculate_distance(problem_type: str, preds_a: pd.Series, preds_b: pd.Series):
    for metric in MetricProblemType:
        if problem_type in metric.value:
            return metric_to_f[metric](preds_a, preds_b)
    raise ValueError(f"unsupported problem type '{problem_type}'")


# TODO: Unaddressed problem types (with frequency):
# 'GRAPH_MATCHING': 393
# 'LINK_PREDICTION': 320
# 'COMMUNITY_DETECTION': 212
# 'CLUSTERING': 38,
# 'COLLABORATIVE_FILTERING': 67
# 'OBJECT_DETECTION': 12
