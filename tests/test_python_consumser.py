import json
from gc import callbacks

from confluent_kafka import Producer, Message, KafkaException
from time import perf_counter
import itertools


def acked(err, msg: Message):
    if not err:
        return msg.topic(), msg.offset()
    return err


def send_messages(producer: Producer, max_messages: int = 1):
    messages = [msg for msg in range(1, max_messages)]

    for batch in itertools.batched(messages, 100_000):

        for _ in batch:
            kwargs = {
                'topic': 'topic',
                'value': json.dumps({'a': 'a', 'b': 'b', 'c': 'c'}),
                'callback': acked
            }
            producer.produce(**kwargs)
        producer.flush(1)


def test_consumer_time():
    producer = Producer({'bootstrap.servers': 'localhost:9092'})
    send_messages(producer, 1_000_000_000)
