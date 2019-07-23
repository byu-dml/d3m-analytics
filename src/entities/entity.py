from abc import ABC, abstractmethod


class Entity(ABC):
    """
    All entities without an ID in this same package (e.g. hyperparam, score, etc.)
    should inherit this abstract class, so they can have a common API for
    common operations and data.
    """

    @abstractmethod
    def is_tantamount_to(self, entity) -> bool:
        """
        Return `True` if `self` is functionally the same, although maybe
        not exactly identical, to `entity`, i.e. if `self` is tantamount
        to `entity`.
        """
        pass


class EntityWithId(Entity):
    """
    All entities having an ID in this same package (e.g. Dataset, Pipeline, etc.)
    should inherit this abstract class, so they can have a common API for
    common operations and data.
    """

    @abstractmethod
    def get_id(self) -> str:
        """
        Returns the entity's underlying field that represents its unique ID.
        The actually underlying field name may vary.
        """
        pass
