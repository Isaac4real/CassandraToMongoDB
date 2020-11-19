from datetime import datetime
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import ordered_dict_factory, SimpleStatement
from pymongo import MongoClient, ASCENDING
import json
from itertools import groupby
class TestCassandraQuery(object):
    def __init__(self, session_cassandra, mongodb):
        self._session_cassandra = session_cassandra
        self._mongodb = mongodb
    def query_cassandra(self):
        query = "SELECT JSON * FROM test_table"
        statement = SimpleStatement(query, fetch_size=10)
        rows = self._session_cassandra.execute(statement)
        records = [json.loads(row['[json]']) for row in rows]
        return records

    def write_mongo(self, collection_name, records):
        x = self._mongodb[collection_name].insert_many(records)
        print(x.inserted_ids)

    def create_index_mongo(self, collection_name):
        self._mongodb[collection_name].create_index([("column", ASCENDING)], name="column_idx")
    
    def generate_collection_name(self, colection):
        return "colection_{}".format(colection)

    def run(self):
        for colection, records in self.query_cassandra():
            collection_name = self.generate_collection_name(colection)
            self.create_index_mongo(collection_name)
            self.write_mongo(collection_name, records)

if __name__ == '__main__':
    # connect to cassandra
    _keyspace = "keyspace_cassandra"
    _cluster_cassandra = Cluster(["localhost"])
    _session_cassandra = _cluster_cassandra.connect(_keyspace)
    _session_cassandra.row_factory = ordered_dict_factory

    # connect to mongo
    _mongo_client = MongoClient("localhost", 27017)
    _mydb = _mongo_client["local_test"]

    TestCassandraQuery(_session_cassandra, _mydb).run()
