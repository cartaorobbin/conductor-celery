"""
Kafka extension for conductor_celery.

This extension provides Kafka integration for the conductor_celery library.
It will only be available if kafka-python is installed.
"""

from typing import Any

from .base import BaseExtension


class KafkaExtension(BaseExtension):
    """Kafka integration extension for conductor_celery."""

    def __init__(self, config: dict[str, Any] | None = None):
        """Initialize Kafka extension."""
        super().__init__(config)
        self._producer = None
        self._consumer = None
        self._topics = []

    def is_available(self) -> bool:
        """Check if Kafka is available."""
        try:
            return True
        except ImportError:
            return False

    def initialize(self) -> None:
        """Initialize Kafka connections."""
        if not self.is_available():
            raise RuntimeError("Kafka is not available. Install kafka-python to use this extension.")

        from kafka import KafkaConsumer, KafkaProducer

        # Get Kafka configuration
        bootstrap_servers = self.get_config("bootstrap_servers", ["localhost:9092"])

        # Initialize producer
        producer_config = self.get_config("producer_config", {})
        self._producer = KafkaProducer(bootstrap_servers=bootstrap_servers, **producer_config)

        # Initialize consumer if topics are specified
        topics = self.get_config("topics", [])
        if topics:
            consumer_config = self.get_config("consumer_config", {})
            self._consumer = KafkaConsumer(*topics, bootstrap_servers=bootstrap_servers, **consumer_config)
            self._topics = topics

    def cleanup(self) -> None:
        """Clean up Kafka connections."""
        if self._producer:
            self._producer.close()
            self._producer = None

        if self._consumer:
            self._consumer.close()
            self._consumer = None

    def send_message(self, topic: str, message: bytes, key: bytes | None = None) -> None:
        """Send a message to a Kafka topic."""
        if not self._producer:
            raise RuntimeError("Kafka producer not initialized")

        future = self._producer.send(topic, message, key=key)
        future.get()  # Wait for the message to be sent

    def get_messages(self, timeout_ms: int = 1000) -> list[Any]:
        """Get messages from Kafka topics."""
        if not self._consumer:
            raise RuntimeError("Kafka consumer not initialized")

        messages = []
        for message in self._consumer.poll(timeout_ms=timeout_ms).values():
            messages.extend(message)

        return messages

    @property
    def producer(self):
        """Get the Kafka producer instance."""
        return self._producer

    @property
    def consumer(self):
        """Get the Kafka consumer instance."""
        return self._consumer
