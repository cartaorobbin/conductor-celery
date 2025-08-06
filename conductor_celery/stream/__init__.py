from kafka import KafkaConsumer, KafkaProducer
from kafka.structs import TopicPartition

class PyramidKafkaWrapper:
    _consumer = None
    _producer = None

    def __init__(self, config):
        self.config = config
        self.bootstrap_servers = config.registry.settings.get(
            "kafka.bootstrap_servers"
        ).split(",")

    @property
    def consumer(self):
        if not self._consumer:
            topics = []

            for topic in set(self.config.get_settings()["kafka.topics"].split()):
                topics.append(TopicPartition(topic, 0))

            consumer = KafkaConsumer(
                bootstrap_servers=self.bootstrap_servers,
                api_version=(3, 5, 1),
                group_id="notification-service",
                auto_offset_reset="earliest",
            )
            consumer.assign(topics)
            self._consumer = consumer

        return self._consumer

    @property
    def producer(self):
        if not self._producer:
            producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers, api_version=(3, 5, 1)
            )
            self._producer = producer

        return self._producer


def includeme(config):
    bootstrap_servers = config.registry.settings.get("kafka.bootstrap_servers").split(
        ","
    )  # kafka1:9091 instance

    config.registry.kafka = PyramidKafkaWrapper(config)