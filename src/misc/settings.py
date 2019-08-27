import os
from enum import Enum
from typing import Dict, List

from dotenv import load_dotenv

# Load environment variables into the environment
load_dotenv()

# Pull environment variables from environment
# for use in the code
CLIENT = os.getenv("CLIENT")
SECRET = os.getenv("SECRET")
API = os.getenv("API")
DATA_ROOT = os.getenv("DATA_ROOT", ".")

DATA_ROOT = os.path.abspath(DATA_ROOT)
print(f"using '{DATA_ROOT}' as DATA_ROOT")


class DataDir(Enum):
    INDEXES_DUMP = os.path.join(DATA_ROOT, "dump/indexes")
    PREDICTIONS_DUMP = os.path.join(DATA_ROOT, "dump/predictions")
    EXTRACTION = os.path.join(DATA_ROOT, "extractions")
    CACHE = os.path.join(DATA_ROOT, "cached-function-calls")


class DefaultFile(Enum):
    EXTRACTION_PKL = "denormalized_entity_maps.pkl"


class Index(Enum):
    PIPELINES = "pipelines"
    BAD_PIPELINE_RUNS = "pipeline_runs_untrusted"
    PIPELINE_RUNS = "pipeline_runs_trusted"
    PROBLEMS = "problems"
    DATASETS = "datasets"


# Other constant values
class Const(Enum):
    PREDICTIONS = "predictions"


elasticsearch_fields: Dict[str, List[str]] = {
    Index.PIPELINES.value: ["name", "digest", "source", "inputs", "outputs", "steps"],
    Index.BAD_PIPELINE_RUNS.value: [
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
    Index.PIPELINE_RUNS.value: [
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
    Index.PROBLEMS.value: ["digest", "name", "problem", "performance_metrics"],
    Index.DATASETS.value: ["digest", "id", "name", "description"],
    Const.PREDICTIONS.value: ["run.results.predictions", "id"],
}
