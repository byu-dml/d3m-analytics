from analytics.databases.d3m_client import D3MDB
from analytics.misc.settings import Index


def count():
    """Count the number of documents in each index of the DB."""
    counts = []
    db = D3MDB()

    for index in Index:
        index_name = index.value
        num_docs_in_index = db.search(index=index_name).count()
        counts.append((index_name, num_docs_in_index))

    print("DB index counts ({index_name}\t{count}):")
    print("***************************************")
    for index, count in counts:
        print(f"{index}\t{count}")


if __name__ == "__main__":
    count()
