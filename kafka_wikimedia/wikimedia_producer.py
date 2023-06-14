"""Python's version of Wikimedia Stream to Open Search project
from Apache Kafka Series - Learn Apache Kafka for Beginners v3:
    https://www.udemy.com/course/apache-kafka/
Java code link:
    https://github.com/conduktor/kafka-beginners-course/tree/main/kafka-basics/src/main/java/io/conduktor/demos/kafka

The module consists of kafka producer that produce messages from Wikimedia stream into kafka topics.

"""
import json
import logging

from confluent_kafka import Producer
from confluent_kafka.serialization import StringSerializer
from sseclient import SSEClient


logging.basicConfig(
    format='%(asctime)s %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    filename='logs/wikimedia_producer.log',
    filemode='w',
)
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def produce():
    """Initializes Kafka Producer for Wikimedia stream.
    
    """
    string_serializer = StringSerializer('utf_8')
    producer = Producer({
        'bootstrap.servers': 'localhost:9092',
        'enable.idempotence': 'true',
        'linger.ms': '20',
        'batch.size': str(32 * 1024),
        'compression.type': 'snappy',
    })
    messages = SSEClient("https://stream.wikimedia.org/v2/stream/recentchange")
    for msg in messages:
        if msg.event == 'message':
            try:
                msg_data = msg.data
                logger.info(json.loads(msg_data))
                producer.produce(
                    topic="wikimedia.recentchange",
                    value=string_serializer(msg_data),
                )
            except ValueError:
                pass


if __name__ == "__main__":
    produce()
