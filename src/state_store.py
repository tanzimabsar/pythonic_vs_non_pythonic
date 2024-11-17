from fontTools.misc.filenames import userNameToFileName
from pymongo import MongoClient, InsertOne, UpdateOne
from collections.abc import MutableMapping

class StateStore(MutableMapping):
    def __init__(self, client=None, collection_name='test', db_name="test", mongo_uri="mongodb://localhost:27017",
                 username=None, password=None, **kwargs):
        self.mongo_uri = f'mongodb://{username}:{password}@localhost:27017?auth=Admin' if username and password else mongo_uri
        self.client = client or MongoClient(
            self.mongo_uri
        )
        self.db = self.client[db_name]
        self.collection = self.db[collection_name]


        try:
            self.client.admin.command("ping")
        except Exception as e:
            raise ConnectionError("Could not connect to the database.") from e

    def __getitem__(self, key):
        """Retrieve an item by its key."""
        result = self.collection.find_one({"key": key})
        if result is None:
            raise KeyError(f"Key '{key}' not found.")
        return result["value"]



    def __setitem__(self, key, value):
        """Set the value for a given key."""
        self.collection.update_one(
            {"key": key},
            {"$set": {"key": key, "value": value}},
            upsert=True
        )

    def insert(self, objects: list[dict]):
        self.collection.insert_many(objects)

    def __delitem__(self, key):
        """Delete an item by its key."""
        result = self.collection.delete_one({"key": key})
        if result.deleted_count == 0:
            raise KeyError(f"Key '{key}' not found.")

    def __iter__(self):
        """Iterate over keys."""
        for item in self.collection.find({}, {"_id": 0, "key": 1}):
            yield item["key"]

    def __len__(self):
        """Return the number of items."""
        return self.collection.count_documents({})

    def __repr__(self):
        """String representation of the MongoDict."""
        items = {item["key"]: item["value"] for item in self.collection.find({}, {"_id": 0})}
        return f"{self.__class__.__name__}({items})"

    def clear(self):
        """Remove all items from the dictionary."""
        self.collection.delete_many({})

    def keys(self):
        """Return a view of the dictionary's keys."""
        return [item["key"] for item in self.collection.find({}, {"_id": 0, "key": 1})]

    def values(self):
        """Return a view of the dictionary's values."""
        return [item["value"] for item in self.collection.find({}, {"_id": 0, "value": 1})]

    def items(self):
        """Return a view of the dictionary's items."""
        return [(item["key"], item["value"]) for item in self.collection.find({}, {"_id": 0})]

    def update(self, other=None, **kwargs):
        """Update the dictionary with the key/value pairs from other."""
        if other:
            for key, value in other.items():
                self[key] = value
        for key, value in kwargs.items():
            self[key] = value
