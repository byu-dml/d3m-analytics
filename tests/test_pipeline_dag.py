import unittest

from src.entities.pipeline_dag import PipelineDAG
from tests.utils import load_test_entities


class TestPipeline(unittest.TestCase):
    def setUp(self):
        test_pipes = load_test_entities()["pipelines"]
        self.pipe_with_edges = test_pipes["pipeline_with_edges"]

    def test_construct_dag(self) -> None:
        expected_edges = set(
            [(0, 1), (1, 2), (2, 5), (2, 6), (2, 8), (5, 6), (5, 8), (6, 8)]
        )
        dag = PipelineDAG(self.pipe_with_edges)
        # Assert the graph has the correct number of nodes.
        self.assertEqual(
            dag.g.number_of_nodes(),
            # Subtract 3 (it should have merged 2 extract by semantic types and 1 horizontal concat)
            len(self.pipe_with_edges.steps) - 3 + len(self.pipe_with_edges.inputs),
        )
        # Assert the graph made the correct edges
        self.assertEqual(expected_edges, set(dag.g.edges))

    def test_profile_dag(self) -> None:
        dag = PipelineDAG(self.pipe_with_edges)
        print(dag.profile())
