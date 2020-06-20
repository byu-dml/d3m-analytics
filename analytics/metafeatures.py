import os

from tqdm import tqdm
from fire import Fire
from d3m.container import Dataset
from d3m.metadata.problem import Problem as D3MProblem
from d3m.metadata import base as metadata_base
from common_primitives.dataset_to_dataframe import DatasetToDataFramePrimitive
from common_primitives.simple_profiler import (
    SimpleProfilerPrimitive,
    Hyperparams as ProfilerHyperparams,
)
from common_primitives.column_parser import (
    ColumnParserPrimitive,
    Hyperparams as ParserHyperparams,
)
from metalearn import Metafeatures

from analytics.databases.aml_client import AMLDB
from analytics.misc.settings import logger, dataset_directories


class Problem:
    """
    Loads and holds the dataset, problem, and dataframe of a D3M problem.
    """

    def __init__(self, directory: str, name: str) -> None:
        self.directory = directory
        self.name = name
        self.path = os.path.join(self.directory, self.name)

        self.problem = D3MProblem.load(f"file://{self.problem_doc_path}")
        self.dataset = Dataset.load(f"file://{self.dataset_doc_path}")
        self.task_keywords = {
            keyword.lower()
            for keyword in self.problem.to_json_structure()["problem"]["task_keywords"]
        }
        self.is_single_input = len(self.problem["inputs"]) == 1
        self.is_single_target = (
            self.is_single_input and len(self.problem["inputs"][0]["targets"]) == 1
        )
        self.df = (
            DatasetToDataFramePrimitive(hyperparams={"dataframe_resource": None})
            .produce(inputs=self.dataset)
            .value
        )
        self.nrows = len(self.df.index)
        self.has_unknown_types = (
            "https://metadata.datadrivendiscovery.org/types/UnknownType"
            in {t for col_types in self.get_column_semantic_types() for t in col_types}
        )

    @property
    def problem_doc_path(self) -> str:
        return os.path.join(self.path, f"{self.name}_problem", "problemDoc.json")

    @property
    def dataset_doc_path(self) -> str:
        return os.path.join(self.path, f"{self.name}_dataset", "datasetDoc.json")

    @property
    def is_tabular(self) -> bool:
        return "tabular" in self.task_keywords

    @property
    def can_have_metafeatures_computed(self) -> bool:
        return self.is_tabular and self.is_single_input and self.is_single_target

    def get_target_column_info(self) -> tuple:
        """
        Get's the column index and column name of the target column for
        this problem's dataset. Breaks if this problem uses more than one
        dataset or target.
        """
        assert self.is_single_input and self.is_single_target
        target = self.problem["inputs"][0]["targets"][0]
        return target["column_index"], target["column_name"]

    def get_column_semantic_types(self) -> list:
        number_of_columns = self.df.metadata.query((metadata_base.ALL_ELEMENTS,))[
            "dimension"
        ]["length"]
        generated_semantic_types = [
            self.df.metadata.query((metadata_base.ALL_ELEMENTS, i))["semantic_types"]
            for i in range(number_of_columns)
        ]
        generated_semantic_types = [sorted(x) for x in generated_semantic_types]

        return generated_semantic_types


def compute_metafeatures(problem: Problem) -> dict:
    """
    Takes a problem from its beginning state (just a `Problem`)
    instance, to the actualy computed metafeatures.
    Performs profiling (if needed), then column type parsing,
    then finally metafeature computation.
    """

    # Define data processors to use.

    profiler = SimpleProfilerPrimitive(
        hyperparams=ProfilerHyperparams.defaults().replace(
            {
                "categorical_max_absolute_distinct_values": None,
                "categorical_max_ratio_distinct_values": 0.1,
            }
        )
    )

    parser = ColumnParserPrimitive(
        hyperparams=ParserHyperparams.defaults().replace(
            {
                "parse_semantic_types": (
                    "http://schema.org/Integer",
                    "http://schema.org/Float",
                    "https://metadata.datadrivendiscovery.org/types/FloatVector",
                    "http://schema.org/DateTime",
                )
            }
        )
    )

    mf_extractor = Metafeatures()

    # Profile and parse the dataframe's data types.
    data = problem.df
    if problem.has_unknown_types:
        profiler.set_training_data(inputs=data)
        profiler.fit()
        data = profiler.produce(inputs=data).value
    data = parser.produce(inputs=data).value

    # Isolate X and y.
    target_col_i, target_col_name = problem.get_target_column_info()
    X = data.remove_columns([target_col_i])
    y = data.select_columns([target_col_i])[target_col_name]

    # Compute the metafeatures.
    return mf_extractor.compute(X, y)


def add_metafeatures(*problems_dirs, max_rows: int) -> None:
    """
    Computes metafeatures for datasets, saving them to their proper
    dataset documents in the AML DB. Only computes metafeatures on
    datasets that are tabular, that have a single target, that don't
    have greater than `max_rows`, and that are the only dataset in their
    companion problem.

    Parameters
    ----------
    problems_dirs : list
        A list of directories containing datasets/problems. Each directory
        should contain dataset/problem directories as its immediate subdirectories.
    max_rows : int
        If a dataset has more than `max_rows`, it will be skipped, i.e. no metafeatures
        will be computed for it.
    """
    aml = AMLDB()

    if len(problems_dirs) == 0:
        problems_dirs = dataset_directories

    for problems_dir in problems_dirs:
        print(f"processing problems dir: {problems_dir}")
        for dataset_name in tqdm(os.listdir(problems_dir)):
            problem_ref = Problem(problems_dir, dataset_name)
            if not problem_ref.can_have_metafeatures_computed:
                continue
            if problem_ref.nrows > max_rows:
                continue

            # Compute the metafeatures and save them to the DB if they
            # computed successfully (computation won't be successful if
            # the dataset has characteristics the metafeatures package
            # doesn't support, like vector valued dataframe cells, or target
            # classes with a single instance).
            try:
                metafeatures = compute_metafeatures(problem_ref)
                dataset_metadata = problem_ref.dataset.metadata.query([])
                # Save the metafeatures with the dataset.
                aml.db.datasets.update_many(
                    filter={
                        "$and": [
                            {"id": dataset_metadata["id"]},
                            {"digest": dataset_metadata["digest"]},
                        ]
                    },
                    update={"$set": {"metafeatures": metafeatures}},
                )
            except Exception:
                logger.exception(
                    "Computing metafeatures for problem "
                    f"{problem_ref.name} was unsuccessful. We are skipping it, "
                    "but here is the stack trace:"
                )


if __name__ == "__main__":
    Fire(add_metafeatures)
