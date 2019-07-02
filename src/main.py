from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search
from settings import CLIENT, SECRET, API, INDEXES

client = Elasticsearch(hosts=[API], http_auth=(CLIENT, SECRET))
s = Search(using=client, index=INDEXES["PIPELINES"])

response = s.execute()

for hit in response:
    print(hit)