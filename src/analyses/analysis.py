from abc import ABC, abstractmethod


class Analysis(ABC):
    """
    All analyses that are run by the `src.analyze` module should inherit
    this abstract class, so they match the `src.analyze` module's API.
    """

    @abstractmethod
    def run(self, dataset: dict):
        pass
