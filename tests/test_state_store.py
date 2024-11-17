import os
import resource
import string
import random
import uuid
from io import StringIO

import pandas as pd
import pytest
import psutil

from src.state_store import StateStore
from pydantic import BaseModel, Field

class Metadata(BaseModel):
    timestamp: str = Field(default_factory=lambda: str(uuid.uuid4()))
    status: str = Field(default_factory=lambda: ''.join(random.choices(string.ascii_letters, k=10)))
    created_at: str = Field(default_factory=lambda: str(uuid.uuid4()))
    updated_at: str = Field(default_factory=lambda: str(uuid.uuid4()))

    created_at_: str = Field(default_factory=lambda: str(uuid.uuid4()))
    updated_at_: str = Field(default_factory=lambda: str(uuid.uuid4()))


    created_at__: str = Field(default_factory=lambda: str(uuid.uuid4()))
    updated_at__: str = Field(default_factory=lambda: str(uuid.uuid4()))

    created_at___: str = Field(default_factory=lambda: str(uuid.uuid4()))
    updated_at___: str = Field(default_factory=lambda: str(uuid.uuid4()))


class User(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    name: str = Field(default_factory=lambda: ''.join(random.choices(string.ascii_letters, k=10)))
    extra: Metadata = Field(default_factory=Metadata)

def test_state_store_not_connect():
    with pytest.raises(ConnectionError):
        store = StateStore(mongo_uri='test', collection_name="test")

def show_usage():
    process = psutil.Process(os.getpid())
    memory_usage = process.memory_info().rss  # in bytes
    print(f"Memory usage: {memory_usage / 1024 / 1024:.2f} MB")

def insert_data(store):
    show_usage()
    users = (User().model_dump() for _ in range(1_000_000))
    store.insert(users)
    show_usage()

def test_bulk_write():
    store = StateStore(collection_name="test",
                       username="root", password="example")
    # create a generator to lower memory footprint
    insert_data(store)


# Set memory limit to 1 GB
def set_memory_limit(limit: int = 1024 * 2):
    soft, hard = resource.getrlimit(resource.RLIMIT_AS)
    resource.setrlimit(resource.RLIMIT_AS, (limit * 1024 * 1024, hard))  # 1GB

@pytest.fixture(autouse=False)
def memory_limit():
    """
    Fixture to enforce a memory limit for the test.
    """
    set_memory_limit()

def efficient(store, chunk=10_000):
    import gc
    show_usage()
    print(f'Inserting data {chunk*10}')
    with pd.read_json('test.json', lines=True, chunksize=chunk) as reader:
        show_usage()
        for index, chunk in enumerate(reader):
            print('Chunk:', index)
            users = [User(**row.to_dict()).model_dump() for index, row in chunk.iterrows()]
            store.insert(users)
            show_usage()
    show_usage()



def inefficient(store, chunk=10_000):
    show_usage()
    print(f'Inserting data {chunk * 10}')
    df = pd.read_json('test.json', lines=True, nrows=chunk*10)
    show_usage()
    users = [User(**row.to_dict()).model_dump() for index, row in df.iterrows()]
    store.insert(users)

def clear(db):

    collections = db.list_collection_names()
    for collection in collections:
        db[collection].drop()
        print(f"Collection '{collection}' has been dropped.")

def test_memory_usage(memory_limit):
    show_usage()
    store = StateStore(collection_name="test",
                       username="root", password="example")
    clear(store.db)

    # create a generator to lower memory footprint
    efficient(store, chunk=100_000)
    show_usage()

