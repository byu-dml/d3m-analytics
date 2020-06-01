from elasticsearch import Elasticsearch

from analytics.misc.settings import API

# Use this client to query the D3M MtL elasticsearch DB with
d3m_client = Elasticsearch(hosts=[API], timeout=60)
