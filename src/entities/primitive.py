from typing import List

from src.entities.entity import Entity, Entity
from src.entities.hyperparam import Hyperparam
from src.entities.data_reference import DataReference
from src.misc.utils import has_path


class Primitive(Entity):
    """
    An object representation of fields about an executed
    D3M primitive that we care about for analysis.
    """

    def __init__(self, pipeline_step: dict):
        if pipeline_step["type"] != "PRIMITIVE":
            raise Exception("invalid pipeline step: must be `type: 'PRIMITIVE'")

        primitive_dict: dict = pipeline_step["primitive"]
        self.name = primitive_dict["name"]
        self.digest = primitive_dict.get("digest")
        self.id = primitive_dict["id"]

        self.python_path = primitive_dict["python_path"]
        last_two_of_path = self.python_path.split(".")[-2:]
        self.short_python_path = ".".join(last_two_of_path)

        self.inputs: List[DataReference] = []
        if has_path(pipeline_step, ["arguments", "inputs", "data"]):
            data = pipeline_step["arguments"]["inputs"]["data"]
            if isinstance(data, list):
                for item in data:
                    self.inputs.append(DataReference(item))
            else:
                # data must be a string
                self.inputs.append(DataReference(data))

        self.outputs_ids: List[str] = []
        if "outputs" in pipeline_step:
            for output in pipeline_step["outputs"]:
                self.outputs_ids.append(output["id"])

        self.hyperparams: List[Hyperparam] = []
        if "hyperparams" in pipeline_step:
            for name, hyperparam_dict in pipeline_step["hyperparams"].items():
                self.hyperparams.append(Hyperparam(name, hyperparam_dict))

    def get_id(self):
        return self.digest

    def is_tantamount_to(self, primitive: "Primitive") -> bool:
        """
        Returns `True` even if the primitives have different hyperparameters
        or digests.
        """
        return self.is_same_kind(primitive) and self.has_same_inputs(primitive)

    def is_same_kind(self, primitive: "Primitive") -> bool:
        """
        Checks if two primitives are the same kind of primitive i.e.
        they are instances of the same primitive but may have different inputs
        or hyperparameters.
        """
        if self.python_path == primitive.python_path:
            return True
        else:
            return False

    def has_same_inputs(self, primitive: "Primitive") -> bool:
        """
        Checks if two primitives have the
        same inputs, which includes checking to make sure they have the
        same input type, step reference, and method reference for each input.
        """
        if len(self.inputs) != len(primitive.inputs):
            return False

        for i, my_input in enumerate(self.inputs):
            their_input = primitive.inputs[i]
            if not my_input.equals(their_input):
                return False

        return True
