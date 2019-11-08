import unittest
import math

import numpy as np

from src.misc.metrics import calculate_output_difference, calculate_cod, calculate_rod
from src.entities.predictions import Predictions
from src.misc.settings import ProblemType


class TestPredictions(unittest.TestCase):
    """
    The crux of each test is a call to self.assertEqual() to check for
    an expected result; self.assertTrue() or self.assertFalse() to verify a
    condition; or self.assertRaises() to verify that a specific exception
    gets raised. These methods are used instead of the assert
    statement so the test runner can accumulate all test results and
    produce a report.
    """

    def setUp(self):
        self.regular_indices = [0, 1, 2, 3, 4, 5]
        self.permuted_indices = [3, 0, 4, 2, 1, 5]
        # Numeric data
        self.num_str_w_empty = Predictions(
            self.regular_indices, ["1.0", "1.1", "1", "", "", "3.98"]
        )
        self.num_str_w_one_empty = Predictions(
            self.regular_indices, ["1.0", "1.1", "1", "5", "", "3.98"]
        )
        self.num_str = Predictions(
            self.regular_indices, ["1", "1.1", "1.0", "4", "4.3", "3.98"]
        )
        self.num = Predictions(self.regular_indices, [1, 1.1, 1.0, 4, 4.3, 3.98])
        self.num_different = Predictions(
            self.regular_indices, [1, 1.0, 1.0, 4.5, 4.3, 3.98]
        )
        self.permuted_num = Predictions(
            self.permuted_indices, [4, 1, 4.3, 1.0, 1.1, 3.98]
        )
        # String data
        self.str_w_empty = Predictions(
            self.regular_indices, ["dog", "cat", "cat", "", "", "horse"]
        )
        self.str = Predictions(
            self.regular_indices, ["dog", "cat", "cat", "horse", "cat", "horse"]
        )
        self.permuted_str = Predictions(
            self.permuted_indices, ["horse", "dog", "cat", "cat", "cat", "horse"]
        )

    def test_parse_nums(self):

        # Numbers should parse as numbers

        self.assertTrue(np.issubdtype(self.num_str.data["values"].dtype, np.number))

        self.assertTrue(
            np.issubdtype(self.num_str_w_empty.data["values"].dtype, np.number)
        )

        self.assertTrue(np.issubdtype(self.num.data["values"].dtype, np.number))

    def test_parse_strs(self):

        # Strings should parse as strings

        # np.dtype("O") is the type for a python object.
        self.assertTrue(np.issubdtype(self.str.data["values"].dtype, np.dtype("O")))

        self.assertTrue(
            np.issubdtype(self.str_w_empty.data["values"].dtype, np.dtype("O"))
        )

    def test_find_common(self):
        common_num_str, common_num_str_w_empty = Predictions.find_common(
            self.num_str, self.num_str_w_empty
        )

        self.assertEqual(common_num_str.shape, common_num_str_w_empty.shape)

        self.assertTrue(common_num_str.equals(self.num_str.data["values"]))

        self.assertTrue(
            common_num_str_w_empty.equals(self.num_str_w_empty.data["values"])
        )

    def test_cod(self):

        # COD should calculate correctly on string/object-based predictions

        str_cod = calculate_cod(
            self.str_w_empty.data["values"], self.str.data["values"]
        )
        self.assertEqual(str_cod, 2 / 6)

        # COD should calculate correctly on numeric predictions

        num_cod_w_nans = calculate_cod(
            self.num_str_w_empty.data["values"], self.num_str_w_one_empty.data["values"]
        )
        self.assertEqual(num_cod_w_nans, 1 / 6)

        num_cod_w_diff_precision = calculate_cod(
            self.num_str_w_empty.data["values"], self.num_str.data["values"]
        )
        self.assertEqual(num_cod_w_diff_precision, 2 / 6)

        # COD should calculate correctly, even if the rows are not all in the same order
        num_cod_w_permuted, _ = calculate_output_difference(
            ProblemType.CLASSIFICATION,
            *Predictions.find_common(self.num, self.permuted_num)
        )
        self.assertEqual(num_cod_w_permuted, 0)

    def test_rod(self):

        # ROD should calculate properly on non-permuted data

        rod = calculate_rod(self.num_str.data["values"], self.num.data["values"])
        self.assertEqual(rod, 0)

        rod_different = calculate_rod(
            self.num.data["values"], self.num_different.data["values"]
        )
        self.assertEqual(rod_different, math.sqrt(0.1 ** 2 + 0.5 ** 2))

        # ROD should calculate properly on data whose rows are
        # not all in the same order.

        rod_w_permuted, _ = calculate_output_difference(
            ProblemType.REGRESSION,
            *Predictions.find_common(self.num, self.permuted_num)
        )
        self.assertEqual(rod_w_permuted, 0)
