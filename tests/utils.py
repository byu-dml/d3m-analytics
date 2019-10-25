import json
from typing import List, Dict, Union, Type

from src.entities.entity import Entity
from src.entities.dataset import Dataset
from src.entities.pipeline import Pipeline
from src.entities.pipeline_run import PipelineRun
from src.entities.problem.problem import Problem

_test_entity_names: Dict[str, Dict[str, Union[Type[Entity], List[str]]]] = {
    'pipelines': {
        'type': Pipeline,
        'names': [
            'simple_pipeline_a',
            'simple_pipeline_b',
            'pipeline_with_subpipelines',
            'simple_pipeline_b_one_off',
            'similar_pipeline_a',
            'similar_pipeline_b',
            'similar_pipeline_c',
            'similar_pipeline_d',
        ]
    },
    'pipeline_runs': {
        'type': PipelineRun,
        'names': [
            'similar_pipeline_run_a',
            'similar_pipeline_run_b',
            'similar_pipeline_run_c',
            'similar_pipeline_run_d',
            'invalid_pipeline_run_a', # Has a state of FAILURE
            'invalid_pipeline_run_b', # Has no predictions
            'invalid_pipeline_run_c', # Phase is FIT, not PRODUCE
            'invalid_pipeline_run_d', # Problem task type is unsupported
        ]
    },
    'problems': {
        'type': Problem,
        'names': [
            'problem_a',
            'problem_b',
            'invalid_problem_b'
        ]
    },
    'datasets': {
        'type': Dataset,
        'names': [
            'dataset_a',
            'dataset_b'
        ]
    }
}

_test_data_path: str = "./tests/data"
_test_data_ext: str = ".json"


def load_test_entities() -> Dict[str, Dict[str, Entity]]:
    """
    Loads the test data entities from their JSON representations.

    Returns
    -------
    A dictionary mapping the names of entity types to dictionaries
    containing the entities.
    """
    entities: Dict[str, Dict[str, Entity]] = {}

    for entity_type, entity_names in _test_entity_names.items():
        entities[entity_type] = {}

        for name in entity_names['names']:
            with open(f"{_test_data_path}/{name}{_test_data_ext}", "r") as f:
                entity_dict: dict = json.load(f)
                entity = entity_names['type'](
                    entity_dict, run_predictions_path=_test_data_path+'/predictions'
                )

                if entity_names['type'] == PipelineRun:
                    entities[entity_type][entity.id] = entity
                else:
                    entities[entity_type][entity.digest] = entity

    return entities

def post_init(maps: Dict[str, Dict[str, Entity]]) -> None:
    """
    Calls post_init() on every Entity in maps. Because many entities
    reference other entities, some of their construction can't be
    performed until all entities have been loaded. This function
    allows them to perform any remaining operations.
    """
    for type, entities in maps.items():
        for id, entity in entities.items():
            entity.post_init(maps)
