from abc import ABC, abstractmethod


class Analysis(ABC):
    """
    All analyses that are run by the `src.analyze` module should inherit
    this abstract class, so they match the `src.analyze` module's API.
    """

    @abstractmethod
    def run(self, dataset: dict, verbose: bool):
        """
        Parameters
        ----------
        dataset : Dict[str,PipelineRun]
            A denormalized extraction of pipeline runs
        verbose : bool
            Whether to report the results of the analysis verbosely.
            Generally, this means that if true, per-pipeline-run results
            will be reported in addition to the standard summary results
            reported at the end of the analysis. 
        """
        pass
