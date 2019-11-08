from abc import ABC, abstractmethod
from typing import Sequence


class Entity(ABC):
    """
    All entities without an ID in this same package (e.g. hyperparam, score, etc.)
    should inherit this abstract class, so they can have a common API for
    common operations and data.
    """

    @abstractmethod
    def is_tantamount_to(self, entity: "Entity") -> bool:
        """
        Return `True` if `self` is functionally the same, although maybe
        not exactly identical, to `entity`, i.e. if `self` is tantamount
        to `entity`.
        """
        pass

    @staticmethod
    def are_lists_tantamount(
        entities_a: Sequence["Entity"], entities_b: Sequence["Entity"]
    ):
        if len(entities_a) != len(entities_b):
            return False

        for entity_a, entity_b in zip(entities_a, entities_b):
            if not entity_a.is_tantamount_to(entity_b):
                return False

        return True


class EntityWithId(Entity):
    """
    All entities having an ID in this same package (e.g. Dataset, Pipeline, etc.)
    should inherit this abstract class, so they can have a common API for
    common operations and data.
    """

    @abstractmethod
    def __init__(self, entity_dict: dict) -> None:
        pass

    @abstractmethod
    def get_id(self) -> str:
        """
        Returns the entity's underlying field that represents its unique ID.
        The actually underlying field name may vary.
        """
        pass

    @abstractmethod
    def post_init(self, entity_maps) -> None:
        """
        Called after all entity_maps is built up by
        src.extract.extract_denormalized. Gives the entity
        a chance to do any initialization it couldn't do
        without the full entity_maps during its constructor call.
        """
        pass
