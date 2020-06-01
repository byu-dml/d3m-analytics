import unittest
from copy import deepcopy

from analytics.entities.problem import Problem
from analytics.entities.dataset import Dataset
from analytics.entities.primitive import Primitive
from analytics.entities.predictions import Predictions
from analytics.entities.pipeline_run import PipelineRunPhase, PipelineRunStatus
from analytics.misc.metrics import MetricProblemType
from analytics.misc.settings import PredsLoadStatus
from tests.utils import load_test_entities, post_init


class TestPipelineRun(unittest.TestCase):
    """
    The crux of each test is a call to self.assertEqual() to check for
    an expected result; self.assertTrue() or self.assertFalse() to verify a
    condition; or self.assertRaises() to verify that a specific exception
    gets raised. These methods are used instead of the assert
    statement so the test runner can accumulate all test results and
    produce a report.
    """

    @classmethod
    def setUpClass(self):
        super(TestPipelineRun, self).setUpClass()

        test_maps = load_test_entities()
        post_init(test_maps)

        self.run_a = test_maps["pipeline_runs"]["similar_pipeline_run_a"]
        self.run_b = test_maps["pipeline_runs"]["similar_pipeline_run_b"]

    def setUp(self):
        self.run_c = deepcopy(self.run_b)

    def test_is_same_problem_and_context_as(self):
        # Assert correctly returns true
        self.assertTrue(self.run_a.is_same_problem_and_context_as(self.run_b))
        self.assertTrue(self.run_b.is_same_problem_and_context_as(self.run_a))

        # Assert a run returns true for itself
        self.assertTrue(self.run_a.is_same_problem_and_context_as(self.run_a))
        self.assertTrue(self.run_b.is_same_problem_and_context_as(self.run_b))

        # Assert a run with a different dataset returns false
        tmp = self.run_c.datasets
        self.run_c.datasets = [
            Dataset({"digest": "different_dataset", "id": None, "name": None})
        ]
        self.assertFalse(self.run_a.is_same_problem_and_context_as(self.run_c))
        self.assertFalse(self.run_c.is_same_problem_and_context_as(self.run_a))
        self.run_c.datasets = tmp

        # Assert a run with a different phase returns false
        tmp = self.run_c.run_phase
        if self.run_c.run_phase.value == "FIT":
            self.run_c.run_phase = PipelineRunPhase("PRODUCE")
        else:
            self.run_c.run_phase = PipelineRunPhase("FIT")
        self.assertFalse(self.run_a.is_same_problem_and_context_as(self.run_c))
        self.assertFalse(self.run_c.is_same_problem_and_context_as(self.run_a))
        self.run_c.run_phase = tmp

        # Assert a run with a different status returns false
        tmp = self.run_c.status
        if self.run_c.status.value == "SUCCESS":
            self.run_c.status = PipelineRunStatus("FAILURE")
        else:
            self.run_c.status = PipelineRunStatus("SUCCESS")
        self.assertFalse(self.run_a.is_same_problem_and_context_as(self.run_c))
        self.assertFalse(self.run_c.is_same_problem_and_context_as(self.run_a))
        self.run_c.status = tmp

        # Assert a run with a different problem returns false
        tmp = self.run_c.problem
        self.run_c.problem = Problem(
            {
                "digest": "different_problem",
                "name": None,
                "problem": {"task_type": "COLLABORATIVE_FILTERING"},
                "performance_metrics": [],
                "inputs": [],
            }
        )
        self.assertFalse(self.run_a.is_same_problem_and_context_as(self.run_c))
        self.assertFalse(self.run_c.is_same_problem_and_context_as(self.run_a))
        self.run_c.problem = tmp

    def test_is_one_step_off_from(self):
        # Assert correctly returns true
        self.assertTrue(self.run_a.is_one_step_off_from(self.run_b))
        self.assertTrue(self.run_b.is_one_step_off_from(self.run_a))

        # Assert a run returns false for itself
        self.assertFalse(self.run_a.is_one_step_off_from(self.run_a))
        self.assertFalse(self.run_b.is_one_step_off_from(self.run_b))

        # Assert that a pipeline with an extra step will return false
        tmp = self.run_c.pipeline
        self.run_c.pipeline = deepcopy(self.run_c.pipeline)
        self.run_c.pipeline.steps.append(
            Primitive(
                {
                    "type": "PRIMITIVE",
                    "primitive": {
                        "digest": "different_primitive",
                        "name": "",
                        "id": "",
                        "python_path": "",
                    },
                }
            )
        )
        self.assertFalse(self.run_c.is_one_step_off_from(self.run_a))

        # Assert that a pipeline with too few steps will return false
        self.run_c.pipeline.steps.pop()
        self.run_c.pipeline.steps.pop()
        self.assertFalse(self.run_c.is_one_step_off_from(self.run_a))
        self.run_c.pipeline = tmp

    def test_load_predictions(self):
        self.run_a.load_predictions()
        self.assertEqual(self.run_a.predictions_status, PredsLoadStatus.USEABLE)
        self.assertTrue(isinstance(self.run_a.predictions, Predictions))

    def test_get_output_difference_from(self):
        self.run_a.load_predictions()
        self.run_b.load_predictions()
        output_diff, output_diff_metric = self.run_a.get_output_difference_from(
            self.run_b
        )

        # Only 1 of 2007 predictions are the same
        self.assertEqual(output_diff, 2006 / 2007)
        self.assertEqual(output_diff_metric, MetricProblemType.COD)

    def test_get_scores_of_common_metrics(self):
        a_b_common_metrics = self.run_a.get_scores_of_common_metrics(self.run_b)

        # Test runs a and b each have a metric of accuracy
        self.assertFalse(a_b_common_metrics.get("ACCURACY", None) is None)

        # These score values are defined in our test data JSON
        self.assertEqual(a_b_common_metrics["ACCURACY"][0].value, 0.17887394120577976)
        self.assertEqual(a_b_common_metrics["ACCURACY"][1].value, 0.13153961136023917)

        # The result of get_scores_of_common_metrics should be
        # empty if there are no common metrics
        self.run_c.scores = []
        a_c_common_metrics = self.run_a.get_scores_of_common_metrics(self.run_c)
        self.assertFalse(bool(a_c_common_metrics))
