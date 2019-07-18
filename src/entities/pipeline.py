from src.entities.primitive import Primitive
from src.entities.data_reference import DataReference
from src.entities.document_reference import DocumentReference
from src.utils import enforce_field, has_path


class Pipeline:
    """
    An object representation of fields from a pipeline JSON document that
    we care about for analysis.
    """

    def __init__(self, pipeline_dict: dict, should_enforce_id: bool):
        self.name = pipeline_dict.get("name")

        enforce_field(should_enforce_id, pipeline_dict, "digest")
        self.digest = pipeline_dict["digest"]
        self.source_name = None
        if has_path(pipeline_dict, ["source", "name"]):
            self.source_name = pipeline_dict["source"]["name"]

        self.inputs: list = []
        for input_dict in pipeline_dict["inputs"]:
            self.inputs.append(input_dict["name"])

        self.outputs: list = []
        for output_dict in pipeline_dict["outputs"]:
            self.outputs.append(DataReference(output_dict["data"]))

        self.steps: list = []
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

    def has_same_steps(self, pipeline: "Pipeline") -> bool:
        """
        Returns `True` if `self` has same steps as `pipeline`, which includes
        the same primitive/sub-pipeline and inputs at each step.
        """
        if len(self.steps) != len(pipeline.steps):
            return False

        for i, my_step in enumerate(self.steps):
            their_step = pipeline.steps[i]

            if type(my_step) != type(their_step):
                return False
            if isinstance(my_step, Primitive):
                if not my_step.is_same_kind_and_inputs(their_step):
                    return False
            elif isinstance(my_step, Pipeline):
                if not my_step.has_same_steps(their_step):
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
            for step in self.steps:
                if isinstance(step, DocumentReference):
                    subpipeline = pipelines[step.digest]
                    # Recurse down in case this pipeline has its own subpipelines
                    subpipeline.dereference_subpipelines(pipelines)
                    step = subpipeline

    def print_steps(self, indent: int = 0):
        for step in self.steps:
            if isinstance(step, Primitive):
                print(("\t" * indent) + step.python_path)
            elif isinstance(step, Pipeline):
                step.print_steps(indent + 1)
            else:
                raise ValueError(f"unsupported step type {type(step)}")
