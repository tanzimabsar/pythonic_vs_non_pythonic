import logging
import uuid

from confluent_kafka import Producer, Consumer, KafkaException

CONFIG = {
    'bootstrap.servers': 'localhost:9092',
}

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

events = [f'Hello Kafka {uuid.uuid4()}' for _ in range(1_000)]


def batch_of(size: int, large_collection: list):
    for index, item in enumerate(range(0, len(large_collection), size)):
        logger.info('Processing sub batch %s', index)
        yield large_collection[item:item + size]


def acked(err, msg):
    if err:
        print("Failed to deliver message: %s: %s" % (str(msg), str(err)))


class EventProducer(object):
    def __init__(self, client=None, config=None):
        self.client = client

        if not self.client:
            self.client = Producer(config)

    def send(self, topic, message):
        try:
            self.client.produce(topic, message, callback=acked)
        except KafkaException as err:
            raise err
        finally:
            self.client.poll(1)

    def send_batch(self, topic, messages):
        [self.send(topic, message) for message in messages]

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        logger.info('Finished context manager. Flushing Kafka Producer internally')
        self.client.flush()


def test_kafka_producer():
    """
    Easily and concisely, transform this 'Bad' API into a 'Good' API.
    By using Python Idioms and PEP 8 Style Guidelines.
    """

    with EventProducer(config=CONFIG) as kafka:
        kafka.send('test', f'Hello Kafka {uuid.uuid4()}')


def test_kafka_microbatch():
    """
    Now We want to take an n length list of messages
    and produce them in a single batch
    of smaller batches of m, where m < n
    """

    for batch in batch_of(100, events):
        with EventProducer(config=CONFIG) as ep:
            ep.send_batch('test', batch)
