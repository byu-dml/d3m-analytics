from elasticsearch import Elasticsearch

from src.misc.settings import CLIENT, SECRET, API

# Use this client to query elasticsearch with
client = Elasticsearch(hosts=[API], http_auth=(CLIENT, SECRET), timeout=60)
