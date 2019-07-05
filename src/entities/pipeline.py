from src.entities.primitive import Primitive
from src.entities.data_reference import DataReference
from src.entities.document_reference import DocumentReference
from src.utils import enforce_field


class Pipeline:
    """
    An object representation of fields from a pipeline JSON document that
    we care about for analysis.
    """

    def __init__(self, pipeline_dict: dict, should_enforce_id: bool):
        self.name = pipeline_dict.get("name")

        enforce_field(should_enforce_id, pipeline_dict, "digest")
        self.digest = pipeline_dict["digest"]

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
