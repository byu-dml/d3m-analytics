from abc import ABC, abstractmethod
from typing import Dict, Any

import pprint


class Analysis(ABC):
    """
    All analyses that are run by the `analytics.analyze` module should inherit
    this abstract class, so they match the `analytics.analyze` module's API.

    Attributes
    ----------
    required_aggregations
        A list of the aggregations that must have been calculated for an
        analysis to be performed. The output files from these aggregations
        are ingested in the `analytics.analyze` module and passed to the analysis's
        run method via the `aggregation_maps` parameter.
    """

    pp = pprint.PrettyPrinter(indent=2).pprint

    required_aggregations = []

    @abstractmethod
    def run(
        self,
        entity_maps: Dict[str, dict],
        verbose: bool,
        refresh: bool,
        aggregations: Dict[str, Any] = None,
    ):
        """
        Parameters
        ----------
        entity_maps
            A dictionary mapping extraction names to extraction dictionaries.
            Contains extraction dictionaries for pipeline runs, problems,
            pipelines, and datasets.
        verbose
            Whether to report the results of the analysis verbosely.
            Generally, this means that if `True`, per-entity results
            will be reported in addition to the standard summary results
            reported at the end of the analysis. It could also mean
            pyplot pop-up windows will appear when `True`.
        refresh
            Whether to refresh any caches that this analysis uses.
        aggregations
            A dictionary mapping aggregation names to the data computed and
            stored by that aggregation. This data can be in any format, varying
            based on the needs of the specific aggregation.
        """
        pass
