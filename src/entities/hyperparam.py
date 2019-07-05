class Hyperparam:
    def __init__(self, name: str, hyperparam: dict):
        self.name = name
        self.type = hyperparam["type"]
        self.data = hyperparam["data"]

    def equals(self, hyperparam: "Hyperparam") -> bool:
        if (
            self.name == hyperparam.name
            and self.type == hyperparam.type
            and self.data == hyperparam.data
        ):
            return True
        else:
            return False

    def is_same_kind(self, hyperparam: "Hyperparam") -> bool:
        """
        Checks if two hyperparams are the same kind of hyperparams i.e.
        they are instances of the same hyperparam but may have different data.
        """
        if self.name == hyperparam.name and self.type == hyperparam.type:
            return True
        else:
            return False
