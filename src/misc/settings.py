from os import getenv
from enum import Enum
from typing import Dict, List

from dotenv import load_dotenv

# Load environment variables into the environment
load_dotenv()

# Pull environment variables from environment
# for use in the code
CLIENT = getenv("CLIENT")
SECRET = getenv("SECRET")
API = getenv("API")


class DefaultDir(Enum):
    DUMP = "dump"
    EXTRACTION = "extractions"
    CACHE = "cached-function-calls"


class DefaultFile(Enum):
    EXTRACTION_PKL = "denormalized_entity_maps.pkl"


class Index(Enum):
    PIPELINES = "pipelines"
    BAD_PIPELINE_RUNS = "pipeline_runs_untrusted"
    PIPELINE_RUNS = "pipeline_runs_trusted"
    PROBLEMS = "problems"
    DATASETS = "datasets"


elasticsearch_fields: Dict[Index, List[str]] = {
    Index.PIPELINES: ["name", "digest", "source", "inputs", "outputs", "steps"],
    Index.BAD_PIPELINE_RUNS: [
        "id",
        "status",
        "start",
        "end",
        "_submitter",
        "run",
        "datasets",
        "pipeline",
        "problem",
    ],
    Index.PIPELINE_RUNS: [
        "id",
        "status",
        "start",
        "end",
        "_submitter",
        "run",
        "datasets",
        "pipeline",
        "problem",
    ],
    Index.PROBLEMS: ["digest", "name", "problem", "performance_metrics"],
    Index.DATASETS: ["digest", "id", "name", "description"],
}
