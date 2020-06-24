from fire import Fire

from analytics.copy import copy_indexes
from analytics.denormalize import extract_denormalized


def sync(batch_size: int = 50) -> None:
    copy_indexes(batch_size=batch_size)
    extract_denormalized(batch_size=batch_size)


if __name__ == "__main__":
    Fire(sync)
