from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search
from tqdm import tqdm

from analytics.misc.settings import API


class D3MDB:
    def __init__(self, timeout: int = 60) -> None:
        # This client is used to query the D3M MtL elasticsearch DB with
        self.es = Elasticsearch(hosts=[API], timeout=timeout)

    def search(self, *args, **kwargs) -> Search:
        """
        Wraps a call to `elasticsearch_dsl.Search` so it doesn't have to be imported
        along with this class. The main argument to pass to search is
        `index` e.g. `index="pipelines"`. See the documentation:
        https://elasticsearch-dsl.readthedocs.io/en/latest/search_dsl.html
        """
        return Search(using=self.es, *args, **kwargs)

    def get_all_ids(self, index: str, *, verbose: bool = True) -> set:
        """
        Returns a set of all document `_id` values in `index`.
        """
        n_docs = self.search(index=index).count()
        return {
            result.meta.id
            for result in tqdm(
                # Only retrieve the metadata of each document.
                self.search(index=index).source(False).scan(),
                total=n_docs,
                disable=not verbose,
            )
        }
