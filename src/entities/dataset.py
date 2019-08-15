from src.utils import enforce_field
from src.entities.entity import Entity, EntityWithId


class Dataset(EntityWithId):
    def __init__(self, dataset_dict: dict, should_enforce_id: bool):
        enforce_field(should_enforce_id, dataset_dict, "digest")
        self.digest = dataset_dict["digest"]
        self.id = dataset_dict["id"]
        self.name = dataset_dict["name"]
        self.description = dataset_dict.get("description")

    def post_init(self, entity_maps) -> None:
        pass

    def __eq__(self, obj):
        if not type(obj) is type(self):
            return False
        # Dataset digest is computed over both dataset description and
        # files as stored in D3M dataset format, so digest is sufficient
        # to describe a dataset.
        return self.digest == obj.digest

    def __hash__(self):
        return hash(self.digest)

    def get_id(self):
        return self.digest

    def is_tantamount_to(self, dataset: "Dataset") -> bool:
        return self.digest == dataset.digest
