from src.analyses.analysis import Analysis


class BasicStatsAnalysis(Analysis):
    def run(self, dataset: dict):
        print(f"The dataset has {len(dataset.keys())} pipeline runs with unique ids.")
        # TODO: Add more basic stats
