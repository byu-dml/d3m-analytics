class DocumentReference:
    def __init__(self, ref_dict: dict):
        self.digest = ref_dict["digest"]
        self.id = ref_dict["id"]
