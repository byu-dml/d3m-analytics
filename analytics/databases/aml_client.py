"""
Client that interfaces with the BYU AML lab's database.
"""
import typing as t
from pprint import pprint

import pymongo
from pymongo.errors import BulkWriteError
from tqdm import tqdm

from analytics.misc.settings import MONGO_HOST, MONGO_PORT, Index


class AMLDB:
    def __init__(self) -> None:
        self.mongo_client = pymongo.MongoClient(MONGO_HOST, MONGO_PORT)
        self.db = self.mongo_client.analytics

    def get_all_ids(self, collection: str, *, verbose: bool = True) -> set:
        n_docs = self.db[collection].estimated_document_count()
        return {
            doc["_id"]
            for doc in tqdm(
                # Only retrieve the _id field of each document.
                self.db[collection].find({}, {"_id": 1}),
                total=n_docs,
                disable=not verbose,
            )
        }

    def bulk_read_write(
        self, read: Index, write: Index, processor: t.Callable, batch_size: int
    ) -> None:
        """Useful for using data from one index to make writes to another.

        Iterates over each document in the `read`, passing each one to `processor`.
        `processor` should return a Pymongo operation that can be passed to
        `pymongo.Collection.bulk_write`. Submits the write operations in batches
        to the `write` collection of the database.
        """
        ops_buffer = []
        read_collection = self.db[read.value]
        write_collection = self.db[write.value]

        try:

            num_docs = read_collection.estimated_document_count()
            for doc in tqdm(
                read_collection.find({}, batch_size=batch_size), total=num_docs
            ):
                ops_buffer.append(processor(doc))
                if len(ops_buffer) == batch_size:
                    # Time to write and flush.
                    write_collection.bulk_write(ops_buffer, ordered=False)
                    ops_buffer.clear()

            # Write and flush any leftovers.
            if len(ops_buffer) > 0:
                write_collection.bulk_write(ops_buffer, ordered=False)
                ops_buffer.clear()

        except BulkWriteError as bwe:
            pprint(bwe.details)
            raise bwe
