"""Python's version of ConsumerDemo.java
from Apache Kafka Series - Learn Apache Kafka for Beginners v3:
    https://www.udemy.com/course/apache-kafka/
Java code link:
    https://github.com/conduktor/kafka-beginners-course/tree/main/kafka-basics/src/main/java/io/conduktor/demos/kafka

"""
import logging

from confluent_kafka import Consumer
from confluent_kafka.serialization import StringDeserializer


logging.basicConfig(
    format='%(asctime)s %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    filename='logs/consumer_demo.log',
    filemode='w',
)
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def consumer_demo():
    string_deserializer = StringDeserializer('utf_8')
    consumer = Consumer({
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'my-python-application',
        'auto.offset.reset': 'earliest',
    })
    logger.info("Kafka Consumer has been initiated.")
    consumer.subscribe(['demo_python'])
    while True:
        record = consumer.poll(1)
        if record is None:
            continue
        if record.error():
            logger.error('Error: %s', record.error())
        logger.info(
            "Key: %s, Value: %s \nPartition: %s, Offset: %s",
            string_deserializer(record.key()),
            string_deserializer(record.value()),
            record.partition(),
            record.offset(),
        )


if __name__ == "__main__":
    consumer_demo()
