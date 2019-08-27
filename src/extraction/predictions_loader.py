import os

from src.entities.pipeline_run_predictions import PipelineRunPredictions
from src.misc.utils import process_json
from src.misc.settings import DataDir


def load_predictions(pipeline_run_id: str) -> PipelineRunPredictions:
    """
    Loads the predictions for a single pipeline run identified by
    `pipeline_run_id`. Loads them from the DB dump.
    """
    path_to_preds = os.path.join(
        DataDir.PREDICTIONS_DUMP.value, f"{pipeline_run_id}.json"
    )
    return process_json(path_to_preds, PipelineRunPredictions)
