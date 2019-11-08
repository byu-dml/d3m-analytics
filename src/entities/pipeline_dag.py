from typing import Union, Dict, List

import networkx as nx
import numpy as np
import matplotlib.pyplot as plt

from src.entities.pipeline import Pipeline
from src.entities.entity import EntityWithId


class PipelineDAG(EntityWithId):
    """
    A representation of a pipeline as a directed graph. Considers all
    inputs and primitive steps as nodes, and uses each primitive step's
    inputs as its incoming edges.
    """

    primitives_to_exclude: List[str] = [
        "d3m.primitives.data_transformation.extract_columns_by_semantic_types.DataFrameCommon",
        "d3m.primitives.data_transformation.construct_predictions.DataFrameCommon",
        "d3m.primitives.data_preprocessing.do_nothing.DSBOX",
        "d3m.primitives.data_transformation.horizontal_concat.DataFrameConcat",
        "d3m.primitives.data_transformation.remove_semantic_types.DataFrameCommon",
        "d3m.primitives.data_preprocessing.do_nothing_for_dataset.DSBOX",
        "d3m.primitives.data_preprocessing.vertical_concatenate.DSBOX",
        "d3m.primitives.data_transformation.replace_semantic_types.DataFrameCommon",
        "d3m.primitives.data_transformation.extract_columns.DataFrameCommon",
        "d3m.primitives.data_transformation.add_semantic_types.DataFrameCommon",
    ]

    def __init__(self, pipeline: Pipeline) -> None:
        self.digest = pipeline.digest
        self.pipeline = pipeline
        self.g = nx.DiGraph()

        # Create initial nodes from inputs
        num_inputs = len(pipeline.inputs)
        for i in range(num_inputs):
            self.g.add_node(i, python_path=None, step_i=None)

        # Create nodes from steps
        for step_i, step in enumerate(pipeline.steps):
            node_i = step_i + num_inputs
            if isinstance(step, Pipeline):
                raise ValueError("PipelineDAG does not support subpipelines")
            self.g.add_node(node_i, python_path=step.python_path, step_i=step_i)
            # Create the edges leading into this node
            for input_data_ref in step.inputs:
                input_step_i = input_data_ref.index
                input_node_i = (
                    input_step_i
                    if input_data_ref.is_input()
                    else input_step_i + num_inputs
                )
                self.g.add_edge(input_node_i, node_i)

        # Remove any orphan nodes, since they don't contribute to the pipeline
        # in any way.
        orphans = list(nx.isolates(self.g))
        if len(orphans) > 0:
            print(
                f"removing orphan nodes {orphans} from pipeline with digest {self.get_id()}"
            )
            self.g.remove_nodes_from(orphans)

        # Remove all the nodes who are primitives we want excluded from
        # the graph. Merge all their inputs and outputs so links
        # from their inputs to their outputs are preserved.

        nodes_to_remove = [
            node
            for node, node_attrs in self.g.nodes(data=True)
            if node_attrs["python_path"] in self.primitives_to_exclude
        ]

        for node in nodes_to_remove:
            in_edges = self.g.in_edges(node)
            out_edges = self.g.out_edges(node)
            # Connect this node's neighbors
            for source, _ in in_edges:
                for _, target in out_edges:
                    self.g.add_edge(source, target)
            # Finally, remove the node.
            self.g.remove_node(node)

    def get_id(self):
        return self.digest

    def is_tantamount_to(self, pipeline_dag: "PipelineDAG") -> bool:
        return nx.is_isomorphic(self.g, pipeline_dag.g)

    def post_init(self, entity_maps) -> None:
        pass

    def profile(self) -> Dict[str, Union[float, int]]:
        """
        Calculates various metrics on the pipeline DAG, storing
        them in a dict.
        """
        try:
            metrics: Dict[str, Union[float, int]] = {}

            metrics["num_nodes"] = self.g.number_of_nodes()
            metrics["num_edges"] = self.g.number_of_edges()
            metrics["density"] = nx.density(self.g)
            metrics["avg_clustering_coef"] = nx.average_clustering(self.g)
            metrics["longest_path_length"] = nx.dag_longest_path_length(self.g)
            metrics["node_connectivity"] = nx.node_connectivity(self.g)
            metrics["avg_shortest_path_length"] = nx.average_shortest_path_length(
                self.g
            )

            node_degrees = np.array([degree for _, degree in self.g.degree])
            metrics["avg_degree"] = node_degrees.mean()
            metrics["std_degree"] = node_degrees.std()

            laplacian = nx.directed_laplacian_matrix(self.g)
            laplacian_eig_vals, _ = np.linalg.eig(laplacian)
            metrics["laplacian_spectral_radius"] = np.max(
                np.absolute(laplacian_eig_vals)
            )

            undirected_g = self.g.to_undirected()
            metrics["diameter"] = nx.diameter(undirected_g)
            metrics["radius"] = nx.radius(undirected_g)
        except Exception as e:
            print(f"error with pipeline {self.get_id()}")
            print(self.g.nodes.data())
            print("underlying pipeline:")
            for step in self.pipeline.steps:
                print(step.python_path)
                for i in step.inputs:
                    print(f"\t{i.index}")
            nx.draw(self.g, with_labels=True)
            plt.show()
            raise e

        return metrics
