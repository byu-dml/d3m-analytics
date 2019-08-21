from typing import Tuple, List, Dict, Callable, Type, Optional, Union
import itertools
import glob
import json

from src.entities.entity import Entity, EntityWithId
from src.entities.primitive import Primitive
from src.entities.data_reference import DataReference
from src.entities.document_reference import DocumentReference
from src.misc.utils import enforce_field, has_path


class Pipeline(EntityWithId):
    """
    An object representation of fields from a pipeline JSON document that
    we care about for analysis.
    """

    def __init__(self, pipeline_dict: dict, should_enforce_id: bool):
        self.name = pipeline_dict.get("name")

        enforce_field(should_enforce_id, pipeline_dict, "digest")
        self.digest = pipeline_dict["digest"]
        self.id = pipeline_dict.get("id")
        self.source_name = None
        if has_path(pipeline_dict, ["source", "name"]):
            self.source_name = pipeline_dict["source"]["name"]

        self.inputs: List[str] = []
        for input_dict in pipeline_dict["inputs"]:
            self.inputs.append(input_dict["name"])

        self.outputs: List[DataReference] = []
        for output_dict in pipeline_dict["outputs"]:
            self.outputs.append(DataReference(output_dict["data"]))

        self.steps: List[Union[Primitive, DocumentReference, Pipeline]] = []
        self.has_subpipeline = False
        for step in pipeline_dict["steps"]:
            step_type = step["type"]
            if step_type == "PRIMITIVE":
                self.steps.append(Primitive(step))
            elif step_type == "SUBPIPELINE":
                self.has_subpipeline = True
                # This reference will be dereferenced later by the loader
                # once all pipelines are available in memory.
                self.steps.append(DocumentReference(step["pipeline"]))
            else:
                raise Exception(f"unsupported pipeline_steps type '{step_type}'")

    def post_init(self, entity_maps) -> None:
        """
        Dereference any subpipelines and flatten out all steps
        and subpipeline steps into `self.steps`.
        """
        self.dereference_subpipelines(entity_maps["pipelines"])

        self.flattened_steps: List[Primitive] = []
        for step in self.steps:
            if isinstance(step, Pipeline):
                self.flattened_steps += step.steps
            else:
                self.flattened_steps.append(step)

    def get_id(self):
        return self.digest

    @property
    def num_steps(self) -> int:
        """
        Recursively counts the number of steps in the pipeline,
        including any subpipelines.
        """
        num_steps: int = 0
        for step in self.steps:
            if isinstance(step, Pipeline):
                num_steps += step.num_steps
            else:
                num_steps += 1
        return num_steps

    def is_tantamount_to(self, pipeline: "Pipeline") -> bool:
        """
        Returns `True` if `self` has same steps as `pipeline`, which includes
        the same primitive/sub-pipeline and inputs at each step.
        """
        if len(self.outputs) != len(pipeline.outputs):
            return False

        if not all(
            my_output.equals(their_output)
            for my_output, their_output in zip(self.outputs, pipeline.outputs)
        ):
            return False

        if len(self.steps) != len(pipeline.steps):
            return False

        for i, my_step in enumerate(self.steps):
            their_step = pipeline.steps[i]

            if type(my_step) != type(their_step):
                return False
            if isinstance(my_step, Primitive) or isinstance(my_step, Pipeline):
                if not my_step.is_tantamount_to(their_step):
                    return False
            else:
                raise ValueError(f"unsupported step type {type(my_step)}")

        return True

    def dereference_subpipelines(self, pipelines: dict):
        """
        Store an actual object pointer to each of this pipeline's subpipelines,
        rather than just a digest string, so the subpipelines can be easily
        accessed programmatically.
        """
        if self.has_subpipeline:
            for i, step in enumerate(self.steps):
                if isinstance(step, DocumentReference):
                    subpipeline = pipelines[step.digest]
                    # Recurse down in case this pipeline has its own subpipelines
                    subpipeline.dereference_subpipelines(pipelines)
                    self.steps[i] = subpipeline

    def print_steps(self, *, use_short_path: bool = False, indent: int = 0):
        for step in self.steps:
            if isinstance(step, Primitive):
                if use_short_path:
                    path = step.short_python_path
                else:
                    path = step.python_path
                print(("\t" * indent) + path)
            elif isinstance(step, Pipeline):
                step.print_steps(use_short_path=use_short_path, indent=indent + 1)
            else:
                raise ValueError(f"unsupported step type {type(step)}")

    def get_num_steps_off_from(self, pipeline: "Pipeline") -> int:
        """
        Gets the number of steps `pipeline` and `self` have that are not identical.
        """
        num_off: int = abs(len(self.steps) - len(pipeline.steps))

        for my_step, their_step in zip(self.steps, pipeline.steps):

            if isinstance(my_step, Primitive) and isinstance(their_step, Primitive):
                if not my_step.is_tantamount_to(their_step):
                    num_off += 1

            elif isinstance(my_step, Pipeline) and isinstance(their_step, Pipeline):
                num_off += my_step.get_num_steps_off_from(their_step)

            elif isinstance(my_step, Primitive) and isinstance(their_step, Pipeline):
                num_off += their_step.num_steps

            elif isinstance(my_step, Pipeline) and isinstance(their_step, Primitive):
                num_off += my_step.num_steps

            else:
                raise ValueError(
                    f"unsupported step types {type(my_step)} and {type(their_step)}"
                )

        return num_off

    def _get_steps_off_from(
        self, our_steps: List[Primitive], their_steps: List[Primitive]
    ) -> List[Tuple[Optional[Primitive], Optional[Primitive]]]:
        steps_off: List[Tuple[Optional[Primitive], Optional[Primitive]]] = []

        for my_step, their_step in itertools.zip_longest(our_steps, their_steps):

            if isinstance(my_step, Primitive) and isinstance(their_step, Primitive):
                if not my_step.is_tantamount_to(their_step):
                    steps_off.append((my_step, their_step))

            elif isinstance(my_step, Pipeline) and isinstance(their_step, Pipeline):
                steps_off += my_step.get_steps_off_from(their_step)

            elif not isinstance(my_step, Pipeline) and isinstance(their_step, Pipeline):
                if isinstance(my_step, Primitive):
                    steps_off += self._get_steps_off_from([my_step], their_step.steps)
                else:
                    steps_off += self._get_steps_off_from([], their_step.steps)

            elif isinstance(my_step, Pipeline) and not isinstance(their_step, Pipeline):
                if isinstance(their_step, Primitive):
                    steps_off += self._get_steps_off_from(my_step.steps, [their_step])
                else:
                    steps_off += self._get_steps_off_from(my_step.steps, [])

            elif my_step is None and isinstance(their_step, Primitive):
                steps_off.append((None, their_step))

            elif isinstance(my_step, Primitive) and their_step is None:
                steps_off.append((my_step, None))

            else:
                raise ValueError(
                    f"unsupported types {type(my_step)} and {type(their_step)}"
                )
        return steps_off

    def get_steps_off_from(
        self, pipeline: "Pipeline"
    ) -> List[Tuple[Optional[Primitive], Optional[Primitive]]]:
        """
        Gets the python paths of the steps `pipeline` and `self` have that are not identical.
        Returns a list of 2-tuples. Each entry is a pair of primitives that mismatch among
        the pipelines.
        """
        return self._get_steps_off_from(self.steps, pipeline.steps)

    @classmethod
    def from_json(cls, path: str, should_enforce_id: bool) -> "Pipeline":
        with open(path, "r") as f:
            return cls(json.load(f), should_enforce_id)

    @classmethod
    def from_json_glob(
        cls, glob_pattern: str, should_enforce_id: bool
    ) -> Dict[str, "Pipeline"]:
        """
        Goes to all files matching `glob_pattern` and
        tries to treat them like a json pipeline definition
        and load them into a map of pipeline digests to 
        constructed `Pipeline` objects.
        """
        pipelines: Dict[str, Pipeline] = {}
        for path in glob.glob(glob_pattern):
            pipeline = cls.from_json(path, should_enforce_id)
            pipelines[pipeline.digest] = pipeline
        return pipelines
