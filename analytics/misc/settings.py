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

# The URL of the elastic search API for querying the DB.
API = "https://metalearning.datadrivendiscovery.org/es/"


@unique
class Index(Enum):
    # Right now we don't sync the primitives index, since their instantiations
    # are already stored in each pipeline.
    PIPELINES = "pipelines"
    PIPELINE_RUNS = "pipeline_runs"
    PROBLEMS = "problems"
    DATASETS = "datasets"


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
