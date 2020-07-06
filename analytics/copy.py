from fire import Fire
from pymongo.operations import ReplaceOne

from analytics.misc.settings import elasticsearch_fields, Index
from analytics.misc.utils import MongoWriteBuffer, chunk
from analytics.databases.d3m_client import D3MDB
from analytics.databases.aml_client import AMLDB


def copy_indexes(
    *index_names, batch_size: int = 50, rewrite: bool = False, request_timeout: int = 60
) -> None:
    """
    Read the data from D3M's database and store it to the lab's database. We
    mirror just a subset of indexes and fields.

    Parameters
    ----------
    indexes : list
        The names of the specific indexes to read from D3M's db. If
        not provided, all indexes will by read.
    batch_size : int
        The number of records to retrieve from D3M's db with each
        network request.
    rewrite : bool
        If `True`, deletes the collections and rereads them from scratch.
        If `False`, only new records will be copied down.
    request_timeout : int
        Number of seconds to wait for a response from elasticsearch.
    """
    d3m_db = D3MDB()
    aml_db = AMLDB()

    if len(index_names) == 0:
        # Copy all by default.
        to_copy = Index
    else:
        to_copy = {Index(name) for name in index_names}

    for index in to_copy:

        index_name = index.value
        aml_collection = aml_db.db[index_name]

        if rewrite:
            print(f"Removing all records in the '{index_name}' collection...")
            aml_collection.delete_many({})

        # Only copy over documents we don't have yet.
        print(f"Determining which documents to copy from index '{index_name}'...")
        d3m_ids = d3m_db.get_all_ids(index_name)
        aml_ids = aml_db.get_all_ids(index_name)
        ids_of_docs_to_copy = list(d3m_ids - aml_ids)
        num_docs_to_copy = len(ids_of_docs_to_copy)

        print(
            (
                f"Now copying subset of index '{index_name}' ({num_docs_to_copy} documents) "
                f"to the AML database..."
            )
        )

        # We'll write the data to the lab db in batches.
        write_buffer = MongoWriteBuffer(aml_collection, batch_size)

        # Iterate over this index in batches, only querying the subset of fields we care about.
        for id_chunk in chunk(ids_of_docs_to_copy, batch_size, show_progress=True):
            hits = (
                d3m_db.search(index=index_name)
                .query("ids", values=list(id_chunk))
                .source(elasticsearch_fields[index])
                .params(size=batch_size, request_timeout=request_timeout)
                .execute()
            )

            for hit in hits:
                doc = hit.to_dict()
                # Mongodb will use the same primary key elastic search does.
                doc["_id"] = hit.meta.id
                write_buffer.queue(
                    # Insert the doc, or if another document already exists with the same _id,
                    # then replace it.
                    ReplaceOne(filter={"_id": doc["_id"]}, replacement=doc, upsert=True)
                )

        # Write and flush any leftovers.
        write_buffer.flush()


if __name__ == "__main__":
    Fire(copy_indexes)
