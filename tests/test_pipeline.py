import unittest

from src.entities.pipeline import Pipeline
from tests.utils import load_test_entities, post_init


class TestPipeline(unittest.TestCase):
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
        super(TestPipeline, self).setUpClass()

        test_entity_maps = load_test_entities()
        post_init(test_entity_maps)

        self.pipe_a = test_entity_maps['pipelines']['simple_pipeline_a']
        self.pipe_b = test_entity_maps['pipelines']['simple_pipeline_b']
        self.pipe_one_off_b = test_entity_maps['pipelines']['simple_pipeline_b_one_off']
        self.pipe_with_subpipelines = test_entity_maps['pipelines']['pipeline_with_subpipelines']

    def test_has_subpipelines(self):
        self.assertTrue(self.pipe_with_subpipelines.has_subpipeline)
        self.assertFalse(self.pipe_a.has_subpipeline)

    def test_get_num_steps_off_from(self):

        # Assert one off pipelines are the same offness whether its
        # b vs. a or a vs. b
        num_b_from_one_off = self.pipe_b.get_num_steps_off_from(self.pipe_one_off_b)
        self.assertEqual(num_b_from_one_off, 1)

        num_one_off_from_b = self.pipe_one_off_b.get_num_steps_off_from(self.pipe_b)
        self.assertEqual(num_one_off_from_b, 1)

        # Assert > 1 offsets are correct
        num_a_from_b = self.pipe_a.get_num_steps_off_from(self.pipe_b)
        self.assertEqual(num_a_from_b, 3)

        num_b_from_a = self.pipe_b.get_num_steps_off_from(self.pipe_a)
        self.assertEqual(num_b_from_a, 3)

        # Assert offsets involving subpipelines calculate correctly
        num_a_from_subpipelines = self.pipe_a.get_num_steps_off_from(
            self.pipe_with_subpipelines
        )
        self.assertEqual(num_a_from_subpipelines, 3)

        num_subpipelines_from_a = self.pipe_with_subpipelines.get_num_steps_off_from(
            self.pipe_a
        )
        self.assertEqual(num_subpipelines_from_a, 3)

        # Assert offset == 0 for identical pipelines
        num_a_from_a = self.pipe_a.get_num_steps_off_from(self.pipe_a)
        self.assertEqual(num_a_from_a, 0)

    def test_get_steps_off_from(self):

        # Assert one off pipelines are the same offness whether its
        # b vs. a or a vs. b
        steps_off_b_from_one_off = self.pipe_b.get_steps_off_from(self.pipe_one_off_b)
        self.assertEqual(len(steps_off_b_from_one_off), 1)
        self.assertEqual(
            steps_off_b_from_one_off,
            [(self.pipe_b.steps[1], self.pipe_one_off_b.steps[1])],
        )

        steps_off_one_off_from_b = self.pipe_one_off_b.get_steps_off_from(self.pipe_b)
        self.assertEqual(len(steps_off_one_off_from_b), 1)
        self.assertEqual(
            steps_off_one_off_from_b,
            [(self.pipe_one_off_b.steps[1], self.pipe_b.steps[1])],
        )

        # Assert > 1 offsets are correct
        steps_off_a_from_b = self.pipe_a.get_steps_off_from(self.pipe_b)
        self.assertEqual(len(steps_off_a_from_b), 3)
        self.assertEqual(
            steps_off_a_from_b,
            [
                (self.pipe_a.steps[1], self.pipe_b.steps[1]),
                (self.pipe_a.steps[2], self.pipe_b.steps[2]),
                (None, self.pipe_b.steps[3]),
            ],
        )

        steps_off_b_from_a = self.pipe_b.get_steps_off_from(self.pipe_a)
        self.assertEqual(len(steps_off_b_from_a), 3)
        self.assertEqual(
            steps_off_b_from_a,
            [
                (self.pipe_b.steps[1], self.pipe_a.steps[1]),
                (self.pipe_b.steps[2], self.pipe_a.steps[2]),
                (self.pipe_b.steps[3], None),
            ],
        )

        # Assert offsets involving subpipelines calculate correctly
        steps_off_a_from_subpipelines = self.pipe_a.get_steps_off_from(
            self.pipe_with_subpipelines
        )
        self.assertEqual(len(steps_off_a_from_subpipelines), 3)
        self.assertEqual(
            steps_off_a_from_subpipelines,
            [
                (self.pipe_a.steps[1], self.pipe_a.steps[0]),
                (None, self.pipe_a.steps[1]),
                (None, self.pipe_a.steps[2]),
            ],
        )

        steps_off_subpipelines_from_a = self.pipe_with_subpipelines.get_steps_off_from(
            self.pipe_a
        )
        self.assertEqual(len(steps_off_subpipelines_from_a), 3)
        self.assertEqual(
            steps_off_subpipelines_from_a,
            [
                (self.pipe_a.steps[0], self.pipe_a.steps[1]),
                (self.pipe_a.steps[1], None),
                (self.pipe_a.steps[2], None),
            ],
        )

        # Assert offset == 0 for identical pipelines
        num_a_from_a = self.pipe_a.get_steps_off_from(self.pipe_a)
        self.assertEqual(len(num_a_from_a), 0)
