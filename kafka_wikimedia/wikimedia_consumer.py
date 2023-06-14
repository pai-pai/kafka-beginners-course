"""Python's version of Wikimedia Stream to Open Search project
from Apache Kafka Series - Learn Apache Kafka for Beginners v3:
    https://www.udemy.com/course/apache-kafka/
Java code link:
    https://github.com/conduktor/kafka-beginners-course/tree/main/kafka-basics/src/main/java/io/conduktor/demos/kafka

The module contains kafka consumer that takes the data from producer and sends it to OpenSearch.

"""
import datetime
import json
import logging
import signal
import sys
import time

from confluent_kafka import Consumer, KafkaException, TopicPartition
from confluent_kafka.serialization import StringDeserializer
from opensearchpy import OpenSearch
from opensearchpy.exceptions import RequestError


logging.basicConfig(
    format='%(asctime)s %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    filename='logs/wikimedia_consumer.log',
    filemode='w',
)
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def consume():
    interrupted = False

    def shutdown_handler(sig, frame):
        """Handles shut down to close consumer properly.
        
        """
        nonlocal interrupted
        logger.info("Detected a shutdown, let's exit from while loop...")
        interrupted = True
        time.sleep(5)
        sys.exit(0)

    signal.signal(signal.SIGINT, shutdown_handler)

    host = "localhost"
    port = 9200
    open_search_client = OpenSearch(
        hosts = [{'host': host, 'port': port}],
        http_compress = True,
    )
    index_name = "wikimedia"
    index_exists = open_search_client.indices.exists(index_name)
    if not index_exists:
        try:
            open_search_client.indices.create()
            logger.info("The Wikimedia Index has been created!")
        except RequestError as err:
            logger.error(
                err.info.get('error', {}).get('reason', "An error occured due index creation."))
    else:
        logger.info("The Wikimedia Index already exits.")

    string_deserializer = StringDeserializer('utf_8')
    consumer = Consumer({
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'consumer-opensearch-demo',
        'auto.offset.reset': 'latest',
        'enable.auto.commit': 'false',
    })
    logger.info("Kafka Consumer has been initiated.")
    consumer.subscribe(["wikimedia.recentchange"])
    try:
        while not interrupted:
            consumed_offsets = {}
            data = []
            max_total_processing_time = datetime.timedelta(seconds=3)
            max_time = datetime.datetime.now() + max_total_processing_time
            max_records_number = 500
            records_count = 0
            while datetime.datetime.now() < max_time and records_count < max_records_number:
                record = consumer.poll(3)
                if record is None:
                    break
                if record.error():
                    logger.error("Error: %s", record.error())
                else:
                    records_count += 1
                    doc = json.loads(string_deserializer(record.value()))
                    action = {'index': {'_index': index_name, '_id': doc.get('meta').get('id')}}
                    data.append(action)
                    data.append(doc)
                consumed_offsets[(record.topic(), record.partition())] = record.offset()
            if data:
                try:
                    responce = open_search_client.bulk(data, index=index_name)
                    logger.info("Inserted %d records.", len(responce.get('items')))
                except RequestError as err:
                    logger.error(
                        err.info.get('error', {})\
                            .get('reason', "An error occured due bulk insertion."))
                else:
                    offsets_to_commit = [TopicPartition(t, p, o + 1)
                                         for ((t, p), o) in consumed_offsets.items()]
                    consumer.commit(offsets=offsets_to_commit, asynchronous=False)
                    logger.info("Offsets have been committed!")
    except KafkaException as error:
        logger.error("Unexpected exception in the consumer: %s", error)
    finally:
        consumer.close()
        open_search_client.close()
        logger.info("The consumer is now gracefully shut down.")


if __name__ == "__main__":
    consume()
