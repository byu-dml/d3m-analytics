"""
Client that interfaces with the BYU AML lab's database.
"""
import pymongo

from analytics.misc.settings import MONGO_HOST, MONGO_PORT


class AMLDB:
    def __init__(self) -> None:
        self.mongo_client = pymongo.MongoClient(MONGO_HOST, MONGO_PORT)
        self.db = self.mongo_client.analytics
