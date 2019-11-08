import unittest

from src.entities.references.data import DataReference


class TestDataReference(unittest.TestCase):
    def test_equality(self) -> None:
        # test single equality
        self.assertEqual(
            DataReference("steps.0.produce"), DataReference("steps.0.produce")
        )
        self.assertNotEqual(
            DataReference("steps.0.produce"), DataReference("steps.1.produce")
        )
        self.assertNotEqual(
            DataReference("steps.0.produce"), DataReference("steps.0.fit")
        )

        # test set equality
        set_a = set(
            [DataReference("steps.0.produce"), DataReference("steps.1.produce")]
        )
        set_b = set(
            [DataReference("steps.0.produce"), DataReference("steps.1.produce")]
        )
        self.assertEqual(set_a, set_b)

        set_c = set(
            [DataReference("steps.0.produce"), DataReference("steps.2.produce")]
        )
        self.assertNotEqual(set_a, set_c)
