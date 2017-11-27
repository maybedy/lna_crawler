from pymongo import MongoClient

def get_client(host="http://localhost", port=27017):
    return MongoClient(host=host, port=port)

client = None
def get_collection(db_name="TEST", collection_name="results"):
    global client
    if not client:
        client = MongoClient(host="127.0.0.1", port=27017)
    collection = client[db_name][collection_name]
    return collection