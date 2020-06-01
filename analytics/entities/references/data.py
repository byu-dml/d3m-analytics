from typing import Optional

from analytics.entities.entity import Entity


class DataReference(Entity):
    """
    Represents a data reference
    (https://metadata.datadrivendiscovery.org/schemas/v0/definitions.json#/definitions/data_reference),
    with the parts broken up so they can be used in code more easily.
    """

    def __init__(self, reference_string: str):
        """
        Example of reference_string: 'steps.0.produce' or 'inputs.1'
        """
        parts = reference_string.split(".")
        self.type: str = parts[0]
        self.index = int(parts[1])
        self.method_name: Optional[str] = parts[2] if len(parts) > 2 else None
        if len(parts) > 3:
            raise ValueError(f"unknown data reference string '{reference_string}'")

    # Source:
    # https://stackoverflow.com/questions/4950155/objects-as-keys-in-python-dictionaries?rq=1
    def _members(self):
        return (self.type, self.index, self.method_name)

    def is_tantamount_to(self, reference: "DataReference") -> bool:
        return self._members() == reference._members()

    def is_input(self) -> bool:
        return self.type == "inputs"

    def is_step(self) -> bool:
        return self.type == "steps"

    def __hash__(self):
        return hash(self._members())

    def __eq__(self, other):
        if not isinstance(other, type(self)):
            return False
        return self._members() == other._members()
