from elasticsearch import Elasticsearch

from analytics.misc.settings import API

# Use this client to query elasticsearch with
client = Elasticsearch(hosts=[API], timeout=60)
