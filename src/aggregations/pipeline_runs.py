from typing import Dict

from src.aggregations.aggregation import Aggregation

class PipelineRunsAggregation(Aggregation):

    def run(self, entity_maps: Dict[str, dict], verbose: bool=False,
            refresh: bool=True, save_table: bool=True):
        """
        A simple aggregation that formats information about pipeline
        runs as tables for exporting.

        Returns
        -------
        An empty dictionary; any information an analysis would need
        regarding pipeline runs can already be accessed directly via
        the entity maps loaded from the extraction.
        """
        pipeline_runs = entity_maps['pipeline_runs']

        if save_table:
            print('Formatting data for writing to table...')

            tables = {
                'pipeline_runs': (
                    ['run_id', 'status', 'start', 'end', 'submitter', 'run_phase',
                     'pipeline_digest', 'problem_digest', 'problem_name',
                     'problem_type', 'problem_subtype'],
                    [],
                    None
                ),
                'datasets_to_runs': (
                    ['dataset_digest', 'dataset_id', 'dataset_name', 'run_id'],
                    [],
                    None
                )
            }

            runs = list(pipeline_runs.values())
            while len(runs) > 0:
                run = runs.pop(0)

                tables['pipeline_runs'][1].append({
                    'run_id': run.id,
                    'status': run.status.value,
                    'start': str(run.start),
                    'end': str(run.end),
                    'submitter': run.submitter,
                    'run_phase': run.run_phase.value,
                    'pipeline_digest': run.pipeline.digest,
                    'problem_digest': run.problem.digest,
                    'problem_name': run.problem.name,
                    'problem_type': run.problem.type.value,
                    'problem_subtype': run.problem.subtype
                })

                while len(run.datasets) > 0:
                    dataset = run.datasets.pop(0)

                    tables['datasets_to_runs'][1].append({
                        'dataset_digest': dataset.digest,
                        'dataset_id': dataset.id,
                        'dataset_name': dataset.name,
                        'run_id': run.id
                    })

            print('Finished formatting data. Records in each table:')
            for table_name, params in tables.items():
                print(f"{table_name}: "+str(len(params[1])))

            for table_name, params in tables.items():
                self.save_table(table_name, *params)

        return {}
