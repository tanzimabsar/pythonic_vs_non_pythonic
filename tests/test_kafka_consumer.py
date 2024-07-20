"""
Idiomatic Python Code embraces using Python Primitives 
to represent data structures. 

Kafka Consumer represents messages in a collection or list
Lets create a construct that is easy to read and understand
"""

from confluent_kafka import Consumer, KafkaException
from typing import Iterable, List
import logging

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


class KafkaConsumer(Iterable):
    def __init__(self, config: dict, topics: List[str], client = None):
        self.client = client
        
        if not client:
            self.client = Consumer(config)
            
        self.client.subscribe(topics)
    
    def __iter__(self):
        return self
    
    def __next__(self):
        try:
            message = self.client.poll(timeout=1.0)
            if message is None:
                raise StopIteration
            if message.error():
                logger.error(f"Consumer error!")
                raise KafkaException(message.error())
            return message.value()
        except KafkaException as e:
            raise StopIteration from e
        finally:
            self.client.close()
    
        
def test_read_messages():
    config = {
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'test',
        # If no offsets are committed, start at the earliest message present
        'auto.offset.reset': 'earliest'
    }
    topics = ['test']
    [print(message) for message in KafkaConsumer(config=config, topics=topics)]