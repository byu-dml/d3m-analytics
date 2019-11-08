from typing import List, Set, Dict

from src.entities.entity import Entity
from src.entities.hyperparam import Hyperparam
from src.entities.references.data import DataReference


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

        self.inputs: Set[DataReference] = set()
        if "hyperparams" in pipeline_step:
            self._add_inputs_from_args_dict(pipeline_step["hyperparams"])
        if "arguments" in pipeline_step:
            self._add_inputs_from_args_dict(pipeline_step["arguments"])

        self.output_ids: List[str] = []
        if "outputs" in pipeline_step:
            for output in pipeline_step["outputs"]:
                self.output_ids.append(output["id"])

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
        return (
            self.is_same_kind(primitive)
            and self.has_same_inputs(primitive)
            and self.has_same_outputs(primitive)
        )

    def is_same_kind(self, primitive: "Primitive") -> bool:
        """
        Checks if two primitives are the same kind of primitive i.e.
        they are instances of the same primitive but may have different inputs
        or hyperparameters.
        """
        if self.id == primitive.id:
            return True
        else:
            return False

    def has_same_inputs(self, primitive: "Primitive") -> bool:
        """
        Checks if two primitives have the
        same inputs, which includes checking to make sure they have the
        same input type, step reference, and method reference for each input.
        """
        return self.inputs == primitive.inputs

    def has_same_outputs(self, primitive: "Primitive") -> bool:
        # We can just use the `==` operator since `Primitive.output_ids`
        # is a list of strings, which are immutable.
        return self.output_ids == primitive.output_ids

    def is_same_position_different_kind(self, primitive: "Primitive") -> bool:
        """
        Returns `True` if `self` and `primitive` have the same inputs and outputs
        but are different primitives.
        """
        return (
            not self.is_same_kind(primitive)
            and self.has_same_inputs(primitive)
            and self.has_same_outputs(primitive)
        )

    def _add_inputs_from_args_dict(self, args_dict: Dict[str, dict]) -> None:
        """
        Collect data references to previous steps used as inputs to
        this primitive step.
        """
        for arg_dict in args_dict.values():
            if arg_dict["type"] == "CONTAINER":
                data = arg_dict["data"]
                if isinstance(data, list):
                    # data field has multiple references
                    for item in data:
                        self.inputs.add(DataReference(item))
                else:
                    # data field has a single reference
                    self.inputs.add(DataReference(data))
