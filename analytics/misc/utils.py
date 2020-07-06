from pymongo.collection import Collection
from tqdm import tqdm


def chunk(sequence, n: int, *, show_progress: bool = False):
    """
    Yield successive `n`-sized chunks from `sequence`. Source:
    https://stackoverflow.com/questions/312443/how-do-you-split-a-list-into-evenly-sized-chunks
    """
    for i in tqdm(range(0, len(sequence), n), disable=not show_progress):
        yield sequence[i : i + n]


class MongoWriteBuffer:
    """
    Wraps the MongoDB `bulk_write` API to make writing
    bulk operations to a collection in batches easier.
    """

    def __init__(self, collection: Collection, batch_size: int) -> None:
        self.collection = collection
        self.batch_size = batch_size
        self.ops_buffer = []

    def queue(self, operation) -> None:
        """
        Add an operation to the queue. Once we reach the
        batch size, we automatically write and flush the buffer.
        """
        self.ops_buffer.append(operation)
        if self.__len__() >= self.batch_size:
            self.flush()

    def __len__(self) -> int:
        return len(self.ops_buffer)

    def flush(self) -> None:
        if self.__len__() > 0:
            self.collection.bulk_write(self.ops_buffer, ordered=False)
            self.ops_buffer.clear()
