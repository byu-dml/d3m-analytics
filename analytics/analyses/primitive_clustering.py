import os
from typing import Dict, Tuple, List, Any
from copy import deepcopy

import numpy as np
from matplotlib import pyplot as plt
from scipy.spatial.distance import squareform
from scipy.cluster.hierarchy import linkage, dendrogram

from analytics.misc.metrics import MetricProblemType
from analytics.misc.settings import DataDir
from analytics.analyses.analysis import Analysis
from analytics.aggregations.primitive_pairs import PrimitivePairComparisonAggregation


def filter_distance_matrix(
    arr: np.ndarray, labels_arr: List[str], cutoff: float
) -> Tuple[np.ndarray, List[str]]:
    """
    Given a distance matrix, this function creates a modified distance
    matrix in which any rows and columns that have no values less than
    `cutoff` have been removed. The zeros on the diagonal are ignored.

    Parameters
    ----------
    arr
        A square numpy array.
    labels_arr
        An array in which the n-th element labels the node represented by
        the n-th row and column of the distance matrix.
    cutoff
        A value indicating the threshold for filtering.

    Returns
    -------
    A tuple in which the first element is the new, filtered distance
    matrix, and the second element is a new labels array that names
    the rows and columns of this new matrix
    """
    new_labels = deepcopy(labels_arr)
    i = 0
    while i < len(arr):
        j = 0
        accepted = False
        while not accepted and j < len(arr[i]):
            if arr[i][j] < cutoff and i != j:
                accepted = True
                break
            j += 1

        if not accepted:
            arr = np.delete(arr, i, 0)
            arr = np.delete(arr, i, 1)
            del new_labels[i]
            i -= 1

        i += 1

    return arr, new_labels


class PrimitiveClusteringAnalysis(Analysis):
    """
    An analysis that clusters primitives by their output difference.
    For each output difference metric, a dendrogram is saved showing
    the clustering of primitives.
    """

    required_aggregations = [PrimitivePairComparisonAggregation]
    CLIP_DISTANCE = 0.05

    def save_dendrogram(self, linkage, labels, metric_label, *, y_cutoff):
        print(f"Saving dendrogram for metric {metric_label}...")

        plt.figure(figsize=(25, 10))
        plt.title("Hierarchical Clustering - " + metric_label)
        plt.xlabel("Primitive Python Path")
        plt.ylabel("Distance")

        dendrogram(linkage, labels=labels, color_threshold=0, leaf_rotation=50.0)

        axes = plt.gca()
        axes.set_ylim([0, y_cutoff])
        plt.setp(axes.get_xticklabels(), ha="right")

        if not os.path.isdir(DataDir.ANALYSIS.value):
            os.makedirs(DataDir.ANALYSIS.value)
        if not os.path.isdir(
            os.path.join(DataDir.ANALYSIS.value, self.__class__.__name__)
        ):
            os.makedirs(os.path.join(DataDir.ANALYSIS.value, self.__class__.__name__))

        plt.savefig(
            os.path.join(
                DataDir.ANALYSIS.value, self.__class__.__name__, metric_label + ".png"
            ),
            bbox_inches="tight",
        )

    def run(
        self,
        entity_maps: Dict[str, dict],
        verbose: bool,
        refresh: bool,
        aggregations: Dict[str, Any] = None,
    ):

        ppcm = aggregations["PrimitivePairComparisonAggregation"]["ppcm"]
        prim_id_to_paths = aggregations["PrimitivePairComparisonAggregation"][
            "prim_id_to_paths"
        ]

        # Here we construct prim_indexes to map each primitive ID
        # to what will be its index in the distance matrix.
        #
        # The final assignment is deliberately indented to be after the
        # last iteration of the loop; the keys of the PPCM are ordered by
        # the primitives' IDs, and the IDs in each pair are themselves
        # ordered. This means that the primitive ID that is last
        # alphanumerically will be the only one never to appear as the
        # first primitive ID in a pair, but it will be the second ID in
        # the final pair. So we know this ID will be bound to prim2 after
        # the final iteration, and we add it to prim_indexes at that point.
        prim_indexes: Dict[str, int] = {}
        ind = 0
        for prim1, prim2 in ppcm.keys():
            for path1 in prim_id_to_paths[prim1]:
                break
            if path1 not in prim_indexes:
                prim_indexes[path1] = ind
                ind += 1
        for path2 in prim_id_to_paths[prim2]:
            break
        prim_indexes[path2] = ind
        ind += 1

        metric_vars: Dict[str, Dict[str, Any]] = {m.name: {} for m in MetricProblemType}
        for metric in metric_vars.keys():
            metric_vars[metric]["distance_matrix"] = np.ones((ind, ind))
            for i in range(ind):
                metric_vars[metric]["distance_matrix"][i][i] = 0

            metric_vars[metric]["found_any"] = False

        keys = list(ppcm.keys())
        while len(keys) > 0:
            prim_pair = keys.pop(0)
            run_diff_list = ppcm.pop(prim_pair)

            if len(run_diff_list) == 0:
                continue

            for metric in metric_vars.keys():
                metric_vars[metric]["found_iteration"] = False
                metric_vars[metric]["total"] = 0
                metric_vars[metric]["num_diffs"] = 0

            while len(run_diff_list) > 0:
                run_diff = run_diff_list.pop(0)

                metric = run_diff.output_difference_metric.name
                metric_vars[metric]["found_any"] = True
                metric_vars[metric]["found_iteration"] = True
                metric_vars[metric]["total"] += run_diff.output_difference
                metric_vars[metric]["num_diffs"] += 1

            for metric in metric_vars.keys():
                if metric_vars[metric]["found_iteration"]:
                    metric_vars[metric]["avg"] = (
                        metric_vars[metric]["total"] / metric_vars[metric]["num_diffs"]
                    )

                    for path1 in prim_id_to_paths[prim_pair[0]]:
                        break
                    for path2 in prim_id_to_paths[prim_pair[1]]:
                        break
                    metric_vars[metric]["distance_matrix"][prim_indexes[path1]][
                        prim_indexes[path2]
                    ] = metric_vars[metric]["avg"]

        for metric in metric_vars.keys():
            if metric_vars[metric]["found_any"]:

                # The distance matrix provided to squareform() must be symmetric
                metric_vars[metric]["distance_matrix"] = np.minimum(
                    metric_vars[metric]["distance_matrix"],
                    metric_vars[metric]["distance_matrix"].T,
                )

                labels = list(prim_indexes.keys())
                # Manually drop the rows/columns of any primitives that aren't close to any others
                metric_vars[metric]["distance_matrix"], labels = filter_distance_matrix(
                    metric_vars[metric]["distance_matrix"], labels, self.CLIP_DISTANCE
                )

                Z = linkage(
                    squareform(metric_vars[metric]["distance_matrix"]), "average"
                )

                self.save_dendrogram(Z, labels, metric, y_cutoff=self.CLIP_DISTANCE)
