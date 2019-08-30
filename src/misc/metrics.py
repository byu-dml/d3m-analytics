from enum import Enum
from typing import Dict, Callable, List, NamedTuple, Set, Tuple

import pandas as pd
import numpy as np

from src.misc.settings import ProblemType


class MetricData(NamedTuple):
    supported_problem_types: List[ProblemType]
    computer: Callable[[pd.Series, pd.Series], float]


def calculate_cod(preds_a: pd.Series, preds_b: pd.Series) -> float:
    """
    COD is the Classifier Output Difference.
    """
    assert preds_a.dtype == preds_b.dtype
    differences = preds_a != preds_b
    return differences.sum() / differences.size


def calculate_rod(preds_a: pd.Series, preds_b: pd.Series) -> float:
    """
    Regressor Output Difference (ROD) is just the euclidian
    distance between the two series of predictions.
    """
    assert preds_a.size == preds_b.size
    return np.linalg.norm(preds_a - preds_b)


class MetricProblemType(Enum):
    COD = MetricData(
        [
            ProblemType.CLASSIFICATION,
            ProblemType.SEMISUPERVISED_CLASSIFICATION,
            ProblemType.VERTEX_CLASSIFICATION,
        ],
        calculate_cod,
    )
    # ROD is "Regressor Output Difference"
    ROD = MetricData(
        [ProblemType.REGRESSION, ProblemType.TIME_SERIES_FORECASTING], calculate_rod
    )

    @classmethod
    def supported_types(cls) -> Set[str]:
        """Return the union of the problem types supported by all metrics"""
        # cls here is the enumeration
        return set(
            problem_type
            for metric in cls
            for problem_type in metric.value.supported_problem_types
        )


def calculate_output_difference(
    problem_type: ProblemType, preds_a: pd.Series, preds_b: pd.Series
) -> Tuple[float, MetricProblemType]:
    # TODO: Unaddressed problem types (with frequency):
    # 'GRAPH_MATCHING': 393
    # 'LINK_PREDICTION': 320
    # 'COMMUNITY_DETECTION': 212
    # 'CLUSTERING': 38,
    # 'COLLABORATIVE_FILTERING': 67
    # 'OBJECT_DETECTION': 12
    for metric in MetricProblemType:
        if problem_type in metric.value.supported_problem_types:
            return metric.value.computer(preds_a, preds_b), metric
    raise ValueError(f"unsupported problem type '{problem_type}'")

