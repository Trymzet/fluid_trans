"""Producer base-class providing common utilites and functionality"""
import logging
import time


from confluent_kafka import KafkaException
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.avro import AvroProducer, CachedSchemaRegistryClient

logger = logging.getLogger(__name__)

SCHEMA_REGISTRY_URL = "http://localhost:8081"


class Producer:
    """Defines and provides common functionality amongst Producers"""

    # Tracks existing topics across all Producer instances
    existing_topics = set()

    def __init__(
        self,
        topic_name,
        key_schema,
        value_schema=None,
        num_partitions=1,
        num_replicas=1,
    ):
        """Initializes a Producer object with basic settings"""
        self.topic_name = topic_name
        self.key_schema = key_schema
        self.value_schema = value_schema
        self.num_partitions = num_partitions
        self.num_replicas = num_replicas
        self._client = None
        self.broker_properties = {"bootstrap.servers": "PLAINTEXT://localhost:9092"}
        self.producer_properties = {
            "bootstrap.servers": self.broker_properties["bootstrap.servers"],
            "compression.type": "snappy",
            "linger.ms": "100",
        }

        # If the topic does not already exist, try to create it
        if self.topic_name not in Producer.existing_topics:
            self.create_topic()
            Producer.existing_topics.add(self.topic_name)

        # TODO: Configure the AvroProducer
        schema_registry = CachedSchemaRegistryClient(SCHEMA_REGISTRY_URL)
        self.producer = AvroProducer(
            self.producer_properties, schema_registry=schema_registry
        )

    def _get_client(self):
        if self._client is None:
            self._client = AdminClient(self.broker_properties)
        return self._client

    def create_topic(self):
        """Creates the producer topic if it does not already exist"""
        client = self._get_client()
        existing_topics = client.list_topics(timeout=5).topics

        # Checks that the topic has not been created using other methods (eg. CLI)
        if self.topic_name not in existing_topics:

            topic = NewTopic(topic=self.topic_name, num_partitions=self.num_partitions)

            try:
                client.create_topics([topic])
            except KafkaException as e:
                msg = f"Oops. Could not create topic {self.topic_name}."
                raise ValueError(msg) from e

            logger.info(f"Topic {self.topic_name} has been created successfully.")

    def delete_topic(self):
        client = self._get_client()

        # Checks that the topic exists
        existing_topics = (
            set(client.list_topics(timeout=5).topics) | Producer.existing_topics
        )
        if self.topic_name not in existing_topics:
            raise ValueError(f"Topic {self.topic_name} does not exist.")

        try:
            client.delete_topics(self.topic_name)
        except KafkaException as e:
            raise ValueError(f"Oops. Could not delete topic {self.topic_name}.") from e

        Producer.existing_topics.remove(self.topic_name)

        logger.info(f"Topic {self.topic_name} has been deleted successfully.")

    def close(self):
        """Prepares the producer for exit by cleaning up the producer"""

        logger.info(f"Closing the producer...")
        self.producer.flush()
        self.delete_topic()
        logger.info(f"The producer has been closed successfully.")

    def time_millis(self):
        """Use this function to get the key for Kafka Events"""
        return int(round(time.time() * 1000))
