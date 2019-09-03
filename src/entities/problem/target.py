from src.entities.entity import Entity


class Target(Entity):
    def __init__(self, target_dict: dict):
        self.column_name: str = target_dict["column_name"]
        self.column_index: int = target_dict["column_index"]

    def is_tantamount_to(self, target: "Target") -> bool:
        return (
            self.column_name == target.column_name
            and self.column_index == target.column_index
        )
