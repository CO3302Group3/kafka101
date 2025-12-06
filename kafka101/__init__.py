"""High level Kafka helpers for microservices."""

from .config import KafkaConfig
from .consumer import KafkaConsumer, KafkaRecord, KafkaSubscriber
from .producer import KafkaProducer, KafkaPublisher
from .serialization import JsonSerializer
from .utility import (
    MessageHandler,
    consume_forever,
    create_publisher,
    create_subscriber,
    publish_message,
)

__all__ = [
    "KafkaConfig",
    "KafkaProducer",
    "KafkaPublisher",
    "KafkaConsumer",
    "KafkaSubscriber",
    "KafkaRecord",
    "JsonSerializer",
    "MessageHandler",
    "create_publisher",
    "create_subscriber",
    "publish_message",
    "consume_forever",
]

__version__ = "0.1.0"
