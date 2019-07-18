from src.utils import enforce_field


class Dataset:
    def __init__(self, dataset_dict: dict, should_enforce_id: bool):
        enforce_field(should_enforce_id, dataset_dict, "digest")
        self.digest = dataset_dict["digest"]
        self.id = dataset_dict["id"]
        self.name = dataset_dict["name"]
        self.description = dataset_dict.get("description")

    # Source: https://stackoverflow.com/questions/4950155/objects-as-keys-in-python-dictionaries?rq=1
    def _members(self):
        return (self.digest, self.id, self.name, self.description)

    def __eq__(self, obj):
        if type(obj) is type(self):
            return self._members() == obj._members()
        else:
            return False

    def __hash__(self):
        return hash(self._members())
