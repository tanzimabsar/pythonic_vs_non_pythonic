import asyncio

from motor.motor_asyncio import AsyncIOMotorClient
from tests.test_state_store import User
import pandas as pd


async def async_insert_data(chunk, db_name='test', collection_name='test', records=None):
    """
    Asynchronously inserts a batch of records into MongoDB.

    :param db_name: The MongoDB database name.
    :param collection_name: The MongoDB collection name.
    :param records: List of dictionaries representing the documents to insert.
    """
    # Initialize the MongoDB async client
    username = 'root'
    password = 'example'
    uri = f'mongodb://{username}:{password}@localhost:27017?auth=Admin'
    client = AsyncIOMotorClient(uri)  # Adjust the URI if needed

    # Get the database and collection
    db = client[db_name]
    collection = db[collection_name]

    # Insert records asynchronously
    records = (User(**row.to_dict()).model_dump() for index, row in chunk.iterrows())
    result = await collection.insert_many(records)
    print(f"Inserted {len(result.inserted_ids)} documents into {collection_name}.")


async def async_insert_data_task(chunks, db_name='test', collection_name='test'):
    """
    Asynchronously inserts multiple chunks of records into MongoDB.

    :param chunks: List of DataFrame chunks to insert.
    :param db_name: The MongoDB database name.
    :param collection_name: The MongoDB collection name.
    """
    tasks = [async_insert_data(chunk, db_name, collection_name) for chunk in chunks]
    await asyncio.gather(*tasks)

def get():
    with pd.read_json('test.json', lines=True, chunksize=100_000) as reader:
        for index, chunk in enumerate(reader):
            yield chunk


def test_insert():
    tasks = list(get())
    asyncio.run(async_insert_data_task(tasks))
