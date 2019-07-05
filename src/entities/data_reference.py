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
        try:
            parts = reference_string.split(".")
        except AttributeError:
            print(reference_string)
            raise AttributeError()
        self.type: str = parts[0]
        self.index: int = int(parts[1])
        self.method_name: Optional[str] = parts[2] if len(parts) > 2 else None
        if len(parts) > 3:
            raise Exception(f"unknown data reference string '{reference_string}'")

