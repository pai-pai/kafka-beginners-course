"""Python's version of ProducerDemo.java
from Apache Kafka Series - Learn Apache Kafka for Beginners v3:
    https://www.udemy.com/course/apache-kafka/
Java code link:
    https://github.com/conduktor/kafka-beginners-course/tree/main/kafka-basics/src/main/java/io/conduktor/demos/kafka

"""
import logging

from confluent_kafka import Producer
from confluent_kafka.serialization import StringSerializer


logging.basicConfig(
    format='%(asctime)s %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    filename='logs/producer_demo.log',
    filemode='w',
)
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def producer_demo():
    string_serializer = StringSerializer('utf_8')
    producer = Producer({
        'bootstrap.servers': 'localhost:9092',
    })
    logger.info("Kafka Producer has been initiated.")
    producer.poll(1)
    producer.produce(
        topic='demo_python',
        value=string_serializer('hello world')
    )
    producer.flush()


if __name__ == "__main__":
    producer_demo()
