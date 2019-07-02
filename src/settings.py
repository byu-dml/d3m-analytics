from os import getenv

from dotenv import load_dotenv

# Load environment variables into the environment
load_dotenv()

# Pull environment variables from environment
# for use in the code
CLIENT = getenv("CLIENT")
SECRET = getenv("SECRET")
API = getenv("API")

INDEXES = {
    "PIPELINES": "pipelines",
    "BAD_PIPELINE_RUNS": "pipeline_runs_untrusted",
    "PIPELINE_RUNS": "pipeline_runs_trusted",
    "PROBLEMS": "problems",
    "DATASETS": "datasets",
}