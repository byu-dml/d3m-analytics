from elasticsearch_dsl import Search

from analytics.databases.d3m_client import d3m_client
from analytics.misc.settings import Index


def count():
    """Count the number of documents in each index of the DB."""
    counts = []

    for index in Index:
        index_name = index.value
        num_docs_in_index = Search(using=d3m_client, index=index_name).count()
        counts.append((index_name, num_docs_in_index))

    print("DB index counts ({index_name}\t{count}):")
    print("***************************************")
    for index, count in counts:
        print(f"{index}\t{count}")


if __name__ == "__main__":
    count()
