from typing import Dict, Union

from tqdm import tqdm

from src.aggregations.aggregation import Aggregation
from src.entities.pipeline_dag import PipelineDAG


class PipelineDAGsAggregation(Aggregation):
    def run(
        self,
        entity_maps: Dict[str, dict],
        verbose: bool = False,
        refresh: bool = True,
        save_table: bool = True,
    ) -> Dict[str, Dict[str, Union[float, int]]]:
        """
        An aggregation that characterizes each pipeline as a
        directed acyclic graph and computes metrics on those graphs.

        Returns
        -------
        A dictionary mapping each pipeline ID to metrics about its pipeline DAG.
        """
        pipelines = entity_maps["pipelines"]
        dag_map: Dict[str, Dict[str, Union[float, int]]] = {}

        print("Building and profiling pipeline DAGs...")
        for pipeline in tqdm(pipelines.values()):
            pipeline_dag = PipelineDAG(pipeline)
            dag_record = pipeline_dag.profile()
            dag_record["pipeline_digest"] = pipeline.get_id()
            dag_record["source_name"] = pipeline.source_name
            dag_map[pipeline.get_id()] = dag_record

        if save_table:
            self.save_table(
                "pipeline_dags",
                [
                    "pipeline_digest",
                    "source_name",
                    "num_nodes",
                    "num_edges",
                    "density",
                    "avg_clustering_coef",
                    "longest_path_length",
                    "node_connectivity",
                    "avg_shortest_path_length",
                    "avg_degree",
                    "std_degree",
                    "laplacian_spectral_radius",
                    "diameter",
                    "radius",
                ],
                dag_map.values(),
                "pipeline_digest",
            )

        return dag_map
