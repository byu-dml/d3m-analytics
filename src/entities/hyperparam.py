from src.entities.entity import Entity


class Hyperparam(Entity):
    def __init__(self, name: str, hyperparam: dict):
        self.name = name
        self.type = hyperparam["type"]
        self.data = hyperparam["data"]

    def is_tantamount_to(self, hyperparam: "Hyperparam") -> bool:
        """
        Checks if two hyperparams are the same kind of hyperparams i.e.
        they are instances of the same hyperparam but may have different data.
        """
        return self.name == hyperparam.name and self.type == hyperparam.type

    def equals(self, hyperparam: "Hyperparam") -> bool:
        return (
            self.name == hyperparam.name
            and self.type == hyperparam.type
            and self.data == hyperparam.data
        )
