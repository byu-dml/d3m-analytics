from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search

from analytics.misc.settings import API


class D3MDB:
    def __init__(self) -> None:
        # This client is used to query the D3M MtL elasticsearch DB with
        self.es = Elasticsearch(hosts=[API], timeout=60)

    def search(self, *args, **kwargs) -> Search:
        """
        Wraps a call to `elasticsearch_dsl.Search` so it doesn't have to imported
        along with this class. The main argument to pass to search is
        `index` e.g. `index="pipelines"`. See the documentation:
        https://elasticsearch-dsl.readthedocs.io/en/latest/search_dsl.html
        """
        return Search(using=self.es, *args, **kwargs)
