"""Python's version of ProducerDemoWithCallback.java
from Apache Kafka Series - Learn Apache Kafka for Beginners v3:
    https://www.udemy.com/course/apache-kafka/
Java code link:
    https://github.com/conduktor/kafka-beginners-course/tree/main/kafka-basics/src/main/java/io/conduktor/demos/kafka

"""
import logging
import time

from confluent_kafka import Producer
from confluent_kafka.serialization import StringSerializer


logging.basicConfig(
    format='%(asctime)s %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    filename='logs/producer_demo_with_callback.log',
    filemode='w',
)
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def producer_demo_with_callback():
    def on_delivery(err, msg):
        """Delivery callback when a message has been
        successfully delivered or permanently failed delivery.
        
        """
        if err is not None:
            logger.error("Error while producing: %s", err)
        else:
            message = f"""Received new metadata
            Topic: {msg.topic()}
            Partition: {msg.partition()}
            Offset: {msg.offset()}
            Timestamp: {msg.timestamp()}"""
            logger.info(message)

    string_serializer = StringSerializer('utf_8')
    producer = Producer({
        'bootstrap.servers': 'localhost:9092',
        'batch.size': '400',
    })
    logger.info("Kafka Producer has been initiated.")
    for j in range(10):
        for i in range(30):
            producer.poll(1)
            producer.produce(
                topic="demo_python",
                value=string_serializer(f"hello world {j}-{i}"),
                callback=on_delivery
            )
            time.sleep(5)
    producer.flush()


if __name__ == "__main__":
    producer_demo_with_callback()
