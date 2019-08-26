from typing import List, Optional, Union

import pandas as pd

from src.misc.utils import has_path, enforce_field
from src.entities.entity import EntityWithId


class PipelineRunPredictions(EntityWithId):
    def __init__(self, pipeline_run_dict: dict, should_enforce_id: bool) -> None:
        enforce_field(should_enforce_id, pipeline_run_dict, "id")

        # Initialize
        self.id = pipeline_run_dict["id"]
        self.prediction_headers: List[str] = []
        self.prediction_indices: Optional[pd.Series[int]] = None
        self.predictions: Optional[
            Union[pd.Series[float], pd.Series[int], pd.Series[str]]
        ] = None
        self.has_useable_predictions = False

        # First, pass all the checks required to determine if this run
        # has predictions we can use. There is some irregularity in the
        # predictions reported in the pipeline runs. We check here for
        # the most popular formatting, and just use predictions for runs
        # that follow that formating scheme i.e. two columns, with one
        # of them being called "d3mIndex", holding the row numbers of the
        # dataset instances predicted on.

        if not has_path(pipeline_run_dict, ["run", "results", "predictions"]):
            return

        predictions_dict = pipeline_run_dict["run"]["results"]["predictions"]

        if "header" not in predictions_dict:
            return

        for col_name in predictions_dict["header"]:
            self.prediction_headers.append(col_name)

        if "values" not in predictions_dict:
            return
        if len(predictions_dict["values"]) != 2:
            return
        if "d3mIndex" not in predictions_dict["header"]:
            return

        # All checks passed. This run has predictions we can use.
        self.has_useable_predictions = True

        i_of_prediction_indices = predictions_dict["header"].index("d3mIndex")
        self.prediction_indices = pd.Series(
            predictions_dict["values"][i_of_prediction_indices]
        )

        # The predictions are just in the column that the prediction indices aren't,
        i_of_predictions = 0 if i_of_prediction_indices == 1 else 1
        self.predictions = pd.Series(predictions_dict["values"][i_of_predictions])

    def get_id(self) -> str:
        return self.id

    def post_init(self, entity_maps) -> None:
        pass

    def is_tantamount_to(self, entity) -> bool:
        raise NotImplementedError
