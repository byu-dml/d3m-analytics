"""
Reads in the exline pipelines, converts them to DAGs, then
calculates metrics on those DAGs, saving the results to CSV.
"""

from src.aggregations.pipeline_dags import PipelineDAGsAggregation
from src.entities.pipeline import Pipeline

entity_maps = {"pipelines": Pipeline.from_json_glob("dump/exlines/*.json")}

aggregator = PipelineDAGsAggregation()

dag_map = aggregator.run(entity_maps, save_table=False)
aggregator.save_table(
    "exline_dags",
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
