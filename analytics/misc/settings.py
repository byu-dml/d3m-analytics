import os
from enum import Enum, unique
from typing import Dict, List
import logging

from dotenv import load_dotenv


logger = logging.getLogger("d3m-analytics")
logger.setLevel("INFO")

# Load environment variables into the environment
load_dotenv()

# Pull environment variables from environment
# for use in the code
MONGO_HOST = os.environ["MONGO_HOST"]
MONGO_PORT = int(os.environ["MONGO_PORT"])
DATA_ROOT = os.getenv("DATA_ROOT", ".")
DATA_ROOT = os.path.abspath(DATA_ROOT)
print(f"using '{DATA_ROOT}' as DATA_ROOT")

# The URL of the elastic search API for querying the DB.
API = "https://metalearning.datadrivendiscovery.org/es/"


@unique
class DataDir(Enum):
    EXTRACTION = os.path.join(DATA_ROOT, "extractions")
    AGGREGATION = os.path.join(DATA_ROOT, "aggregations")
    ANALYSIS = os.path.join(DATA_ROOT, "analyses")
    CACHE = os.path.join(DATA_ROOT, "cached-function-calls")


@unique
class DefaultFile(Enum):
    EXTRACTION_PKL = "denormalized_entity_maps.pkl"


@unique
class Index(Enum):
    # Right now we don't sync the primitives index, since their instantiations
    # are already stored in each pipeline.
    PIPELINES = "pipelines"
    PIPELINE_RUNS = "pipeline_runs"
    PROBLEMS = "problems"
    DATASETS = "datasets"


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


# The fields to sync for each collection
# from the elasticsearch instance.
elasticsearch_fields: Dict[str, List[str]] = {
    Index.PIPELINES: [
        "_id",
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
    Index.PIPELINE_RUNS: [
        "_id",
        "id",
        "status",
        "start",
        "end",
        "_submitter",
        # We drill in on "run" because it has several large
        # fields we don't need, but some we do.
        "run.phase",
        "run.results.scores",
        # FYI: This field is HUGE
        # "run.results.predictions",
        "datasets",
        "pipeline",
        "problem",
    ],
    Index.PROBLEMS: [
        "_id",
        "id",
        "digest",
        "name",
        "problem",
        "performance_metrics",
        "inputs",
    ],
    Index.DATASETS: ["_id", "digest", "id", "name", "description"],
}

# Directories in the AML lab file server that house dataset
# we use.
dataset_directories = [
    "/users/data/d3m/datasets/training_datasets/LL0",
    "/users/data/d3m/datasets/training_datasets/LL1",
    "/users/data/d3m/datasets/seed_datasets_current",
    "/users/data/d3m/datasets/training_datasets/seed_datasets_archive",
]
