from typing import Optional


class DataReference:
    """
    Represents a data reference (https://metadata.datadrivendiscovery.org/schemas/v0/definitions.json#/definitions/data_reference),
    with the parts broken up so they can be used in code more easily.
    """

    def __init__(self, reference_string: str):
        """
        Example of reference_string: 'steps.0.produce' or 'inputs.1'
        """
        parts = reference_string.split(".")
        self.type: str = parts[0]
        self.index: int = int(parts[1])
        self.method_name: Optional[str] = parts[2] if len(parts) > 2 else None
        if len(parts) > 3:
            raise ValueError(f"unknown data reference string '{reference_string}'")

    # Source: https://stackoverflow.com/questions/4950155/objects-as-keys-in-python-dictionaries?rq=1
    def _members(self):
        return (self.type, self.index, self.method_name)

    def __eq__(self, obj):
        if type(obj) is type(self):
            return self._members() == obj._members()
        else:
            return False

    def __hash__(self):
        return hash(self._members())
