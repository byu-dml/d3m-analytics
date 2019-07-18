from src.entities.dataset import Dataset
from src.extraction.loader import load_index
from src.settings import Indexes


def load_datasets(dump_path: str, should_enforce_id: bool) -> dict:
    """
    Loads a map of datasets from the dump_path.
    Returns a dictionary map of each dataset digest to its dataset.
    """
    datasets = {}

    for dataset_dict in load_index(dump_path, Indexes.DATASETS.value):
        dataset = Dataset(dataset_dict, should_enforce_id)
        datasets[dataset.digest] = dataset

    return datasets
