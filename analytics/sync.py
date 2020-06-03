from tqdm import tqdm
from fire import Fire
from pymongo.operations import ReplaceOne

from analytics.misc.settings import elasticsearch_fields, Index
from analytics.databases.d3m_client import D3MDB
from analytics.databases.aml_client import AMLDB


def sync_indexes(batch_size: int = 100, indexes: list = None) -> None:
    """
    Read the data from D3M's database and sync it to the lab's database. We sync
    a subset of indexes and fields.

    Parameters
    ----------
    batch_size : int
        The number of records to retrieve from D3M's db with each
        network request.
    indexes : list
        The names of the specific indexes to sync from D3M's db. If
        `None`, all indexes will by synced.
    """
    d3m_db = D3MDB()
    aml_db = AMLDB()
    should_index_all = indexes is None

    for index in Index:
        if should_index_all or index.value in indexes:

            index_name = index.value
            aml_collection = aml_db.db[index_name]

            # Get number of docs in D3M index
            s = d3m_db.search(index=index_name)
            num_docs_in_index = s.count()
            print(
                (
                    f"Now syncing index '{index_name}' ({num_docs_in_index} documents) "
                    f"to the AML database..."
                )
            )

            # Remove all records currently in the `index_name` collection.
            aml_collection.delete_many({})

            ops_buffer = []
            # Iterate over this index in batches, only querying the subset of fields we care about.
            # Write the data to the lab's db in batches as well.
            scanner = (
                s.source(elasticsearch_fields[index]).params(size=batch_size).scan()
            )
            for hit in tqdm(scanner, total=num_docs_in_index):
                doc = hit.to_dict()
                # Mongodb will use the same primary key elastic search does.
                doc["_id"] = hit.meta.id
                ops_buffer.append(
                    # Insert the doc, or if another document already exists with the same _id,
                    # then replace it.
                    ReplaceOne(filter={"_id": doc["_id"]}, replacement=doc, upsert=True)
                )

                if len(ops_buffer) == batch_size:
                    # Time to write and flush.
                    aml_collection.bulk_write(ops_buffer, ordered=False)
                    ops_buffer.clear()

            # Write and flush any leftovers.
            if len(ops_buffer) > 0:
                aml_collection.bulk_write(ops_buffer, ordered=False)
                ops_buffer.clear()


if __name__ == "__main__":
    Fire(sync_indexes)
