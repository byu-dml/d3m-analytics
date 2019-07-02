from elasticsearch import Elasticsearch

from settings import CLIENT, SECRET, API, INDEXES

# Use this client to query elasticsearch with
client = Elasticsearch(hosts=[API], http_auth=(CLIENT, SECRET))