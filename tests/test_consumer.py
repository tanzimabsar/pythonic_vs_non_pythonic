"""
Idiomatic Python Code embraces using Python Primitives 
to represent data structures. 

Kafka Consumer represents messages in a collection or list
Lets create a construct that is easy to read and understand
"""
import uuid
from unittest.mock import Mock, patch

from confluent_kafka import Consumer, KafkaException, KafkaError
from typing import Iterable, List
import logging

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

config = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': str(uuid.uuid4()),
    # If no offsets are committed, start at the earliest message present
    'auto.offset.reset': 'earliest'
}
topics = ['test']


class Queue(object):
    def __init__(self, client: Consumer = None, size: int = 10):
        self.size = size
        self.messages = []
        self.client = client

        if not client:
            raise Exception('Send underlying Kafka Consumer')

        self.client.subscribe(topics)

    def __iter__(self):
        """
        Immediately Poll and wait for n seconds
        Fill up the internal queue and commit
        """

        logger.info(f'Hydrating internal queue of {self.size}')

        while True:
            if len(self.messages) >= self.size:
                break

            msg = self.client.poll(timeout=1.0)
            if msg is None:
                continue
            elif msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    raise KafkaException(msg.error())

            else:
                offset, key, val = msg.offset(), msg.key(), msg.value()
                decoded = val.decode('utf-8')
                self.messages.append(decoded)

        try:
            for item in self.messages:
                yield item
        finally:
            logger.info('Committing offsets at the end of consumption')
            self.client.commit()

    def __len__(self):
        return len(self.messages)

    def __next__(self):
        return self


def test_read_messages():
    consumer = Consumer(config)
    consumer.subscribe(topics)

    messages = Queue(consumer, size=10)
    assert len(list(messages)) == 10
