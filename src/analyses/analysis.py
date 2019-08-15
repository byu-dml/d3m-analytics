from abc import ABC, abstractmethod
from typing import Dict

from src.entities.entity import EntityWithId


class Analysis(ABC):
    """
    All analyses that are run by the `src.analyze` module should inherit
    this abstract class, so they match the `src.analyze` module's API.
    """

    @abstractmethod
    def run(self, entity_maps: Dict[str, dict], verbose: bool):
        """
        Parameters
        ----------
        entity_maps : Dict[str, dict]
            A dictionary mapping extraction names to extraction dictionaries.
            Contains extraction dictionaries for pipeline runs, problems,
            pipelines, and datasets.
        verbose : bool
            Whether to report the results of the analysis verbosely.
            Generally, this means that if true, per-entity results
            will be reported in addition to the standard summary results
            reported at the end of the analysis. 
        """
        pass
