from abc import ABC, abstractmethod
from typing import Dict, Callable

import pprint

from src.entities.entity import EntityWithId


class Analysis(ABC):
    """
    All analyses that are run by the `src.analyze` module should inherit
    this abstract class, so they match the `src.analyze` module's API.
    """

    pp = pprint.PrettyPrinter(indent=2).pprint

    @abstractmethod
    def run(self, entity_maps: Dict[str, dict], verbose: bool, refresh: bool):
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
        """
        pass
