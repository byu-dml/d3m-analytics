import os
from enum import Enum, unique
from typing import Dict, List

from dotenv import load_dotenv

# Load environment variables into the environment
load_dotenv()

# Pull environment variables from environment
# for use in the code
DATA_ROOT = os.getenv("DATA_ROOT", ".")
DATA_ROOT = os.path.abspath(DATA_ROOT)
print(f"using '{DATA_ROOT}' as DATA_ROOT")

# The URL of the elastic search API for querying the DB.
API = "https://metalearning.datadrivendiscovery.org/es/"


@unique
class DataDir(Enum):
    INDEXES_DUMP = os.path.join(DATA_ROOT, "dump/indexes")
    PREDICTIONS_DUMP = os.path.join(DATA_ROOT, "dump/predictions")
    EXTRACTION = os.path.join(DATA_ROOT, "extractions")
    CACHE = os.path.join(DATA_ROOT, "cached-function-calls")


@unique
class DefaultFile(Enum):
    EXTRACTION_PKL = "denormalized_entity_maps.pkl"


@unique
class Index(Enum):
    PIPELINES = "pipelines"
    BAD_PIPELINE_RUNS = "pipeline_runs_untrusted"
    PIPELINE_RUNS = "pipeline_runs_trusted"
    PROBLEMS = "problems"
    DATASETS = "datasets"


@unique
class PredsLoadStatus(Enum):
    """
    The possible statuses of a pipeline
    run object regarding whether its predictions
    have loaded.
    """

    # We haven't tried loading them
    NOT_TRIED = 0
    # We loaded them and they're useable
    USEABLE = 1
    # We tried loading them but they're either not
    # useable or they don't exist
    NOT_USEABLE = 2


@unique
class ProblemType(Enum):
    CLASSIFICATION = "CLASSIFICATION"
    SEMISUPERVISED_CLASSIFICATION = "SEMISUPERVISED_CLASSIFICATION"
    VERTEX_CLASSIFICATION = "VERTEX_CLASSIFICATION"
    REGRESSION = "REGRESSION"
    TIME_SERIES_FORECASTING = "TIME_SERIES_FORECASTING"
    GRAPH_MATCHING = "GRAPH_MATCHING"
    LINK_PREDICTION = "LINK_PREDICTION"
    COMMUNITY_DETECTION = "COMMUNITY_DETECTION"
    CLUSTERING = "CLUSTERING"
    COLLABORATIVE_FILTERING = "COLLABORATIVE_FILTERING"
    OBJECT_DETECTION = "OBJECT_DETECTION"


@unique
# Other constant values
class Const(Enum):
    PREDICTIONS = "PREDICTIONS"


_pipeline_run_es_fields = [
    "id",
    "status",
    "start",
    "end",
    "_submitter",
    # We drill in on "run" because it has several large
    # fields we don't need, but some we do.
    "run.phase",
    "run.results.scores",
    "run.results.predictions.header",
    "datasets",
    "pipeline",
    "problem",
]

# The fields to dump for each collection
# from the elasticsearch instance.
elasticsearch_fields: Dict[str, List[str]] = {
    Index.PIPELINES.value: [
        "name",
        "id",
        "schema",
        "created",
        "digest",
        "source",
        "inputs",
        "outputs",
        "steps",
    ],
    Index.BAD_PIPELINE_RUNS.value: _pipeline_run_es_fields,
    Index.PIPELINE_RUNS.value: _pipeline_run_es_fields,
    Index.PROBLEMS.value: [
        "digest",
        "name",
        "problem",
        "performance_metrics",
        "inputs",
    ],
    Index.DATASETS.value: ["digest", "id", "name", "description"],
    Const.PREDICTIONS.value: ["run.results.predictions", "id"],
}
