"""Python's version of ConsumerDemo.java
from Apache Kafka Series - Learn Apache Kafka for Beginners v3:
    https://www.udemy.com/course/apache-kafka/
Java code link:
    https://github.com/conduktor/kafka-beginners-course/tree/main/kafka-basics/src/main/java/io/conduktor/demos/kafka

"""
import logging
import signal
import sys
import time

from confluent_kafka import Consumer, KafkaException
from confluent_kafka.serialization import StringDeserializer


logging.basicConfig(
    format='%(asctime)s %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    filename='logs/consumer_demo_with_shutdown.log',
    filemode='w',
)
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def consumer_demo_with_shutdown():
    interrupted = False

    def shutdown_handler(sig, frame):
        """Handles shut down to close consumer properly.
        
        """
        nonlocal interrupted
        logger.info("Detected a shutdown, let's exit from while loop...")
        # Can not completely repeat java's implementation because
        # wake up the consumer poll is not (yet) exposed in the Python client.
        # Until it is the recommendation is to use a lower poll timeout, e.g., 100 ms.
        interrupted = True
        time.sleep(5)
        sys.exit(0)

    signal.signal(signal.SIGINT, shutdown_handler)

    string_deserializer = StringDeserializer('utf_8')
    consumer = Consumer({
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'my-python-application',
        'auto.offset.reset': 'earliest',
    })
    logger.info("Kafka Consumer has been initiated.")
    consumer.subscribe(['demo_python'])
    try:
        while not interrupted:
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
    except KafkaException as error:
        logger.error("Unexpected exception in the consumer: %s", error)
    finally:
        consumer.close()
        logger.info("The consumer is now gracefully shut down.")


if __name__ == "__main__":
    consumer_demo_with_shutdown()
