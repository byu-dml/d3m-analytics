import unittest
from typing import List

from analytics.aggregations.primitive_pairs import (
    PrimitivePairComparisonAggregation,
    PipelineRunPairDiffEntry,
)
from tests.utils import load_test_entities, post_init


class TestPrimitivePairAggregation(unittest.TestCase):
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
        super(TestPrimitivePairAggregation, self).setUpClass()

        self.test_maps = load_test_entities()
        post_init(self.test_maps)
        agg = PrimitivePairComparisonAggregation()
        self.agg_result = agg.run(self.test_maps, save_table=False)

    def _contains_invalid_run(self, diff_list: List[PipelineRunPairDiffEntry]):
        """
        Given a list of pipeline run diff entries, this function determines
        whether any of the diffs include one of the invalid runs in the test
        data (all of them include the word 'invalid' in their ID).
        """
        for d in diff_list:
            if "invalid" in d.run_a.id or "invalid" in d.run_b.id:
                return True
        return False

    def test_ppcm(self):
        ppcm = self.agg_result["ppcm"]

        for prim_pair, diff_list in ppcm.items():
            if len(diff_list) == 0:
                continue

            self.assertFalse(self._contains_invalid_run(diff_list))

    def test_primitive_ids(self):
        self.assertTrue("SIMILAR_PRIMITIVE_A" in self.agg_result["prim_id_to_paths"])
        self.assertTrue("SIMILAR_PRIMITIVE_A" in self.agg_result["prim_ids"])

    def test_ppcm_ordering(self):
        """
        This function is explicitly for testing the ordering of primitive
        pairs and runs in the PPCM. E.g., there should only be entries for
        primitive pairs in the correct order; `run_a` should always correspond
        to the first listed primitive; diffs have a positive value if `run_a`
        had better performance; etc.
        """
        ppcm = self.agg_result["ppcm"]

        for prim_pair, diff_list in ppcm.items():

            if not (
                (
                    prim_pair[0] == "SIMILAR_PRIMITIVE_A"
                    and prim_pair[1] == "SIMILAR_PRIMITIVE_B"
                )
                or (
                    prim_pair[0] == "SIMILAR_PRIMITIVE_C"
                    and prim_pair[1] == "SIMILAR_PRIMITIVE_D"
                )
            ):
                # Only the correct primitive pairs—in the correct order—
                # should have any diffs
                self.assertEqual(len(diff_list), 0)
                continue

            # Given our test data, we should have a diff entry in the
            # (A,B) and (C,D) pairs
            self.assertTrue(len(diff_list) > 0)

            diff = diff_list[0]
            pipe_a = diff.run_a.pipeline
            pipe_b = diff.run_b.pipeline

            self.assertEqual(len(pipe_a.steps), len(pipe_b.steps))
            for i in range(len(pipe_a.steps)):
                if not pipe_a.steps[i].is_tantamount_to(pipe_b.steps[i]):
                    # Assert that the runs in the diff entry were ordered
                    # correctly
                    self.assertTrue(
                        (
                            pipe_a.steps[i].id == "SIMILAR_PRIMITIVE_A"
                            and pipe_b.steps[i].id == "SIMILAR_PRIMITIVE_B"
                        )
                        or (
                            pipe_a.steps[i].id == "SIMILAR_PRIMITIVE_C"
                            and pipe_b.steps[i].id == "SIMILAR_PRIMITIVE_D"
                        )
                    )
                    break

            # Our test data is such that the diff for (A,B) should be
            # negative, and the diff for (C,D) should be positive
            if (
                prim_pair[0] == "SIMILAR_PRIMITIVE_A"
                and prim_pair[1] == "SIMILAR_PRIMITIVE_B"
            ):
                self.assertTrue(diff.score_diffs[0].metric_score_diff < 0)
            if (
                prim_pair[0] == "SIMILAR_PRIMITIVE_C"
                and prim_pair[1] == "SIMILAR_PRIMITIVE_D"
            ):
                self.assertTrue(diff.score_diffs[0].metric_score_diff > 0)
