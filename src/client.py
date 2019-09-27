from elasticsearch import Elasticsearch

from src.misc.settings import API

# Use this client to query elasticsearch with
client = Elasticsearch(hosts=[API], timeout=60)
