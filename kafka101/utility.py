"""Convenience functions for working with kafka101."""

from __future__ import annotations

from typing import Any, Callable, Optional, Sequence

from .config import KafkaConfig
from .consumer import KafkaRecord, KafkaSubscriber
from .producer import KafkaPublisher

MessageHandler = Callable[[KafkaRecord], None]


def create_publisher(
    topic: str,
    *,
    config: Optional[KafkaConfig] = None,
    **kwargs: Any,
) -> KafkaPublisher:
    """Return a KafkaPublisher configured for the provided topic."""

    return KafkaPublisher(topic=topic, config=config, **kwargs)


def create_subscriber(
    topics: Sequence[str] | str,
    *,
    config: Optional[KafkaConfig] = None,
    **kwargs: Any,
) -> KafkaSubscriber:
    """Return a KafkaSubscriber configured for the provided topics."""

    return KafkaSubscriber(topics=topics, config=config, **kwargs)


def publish_message(
    topic: str,
    value: Any,
    *,
    config: Optional[KafkaConfig] = None,
    wait_for_delivery: bool = True,
    publisher_options: Optional[dict] = None,
    publish_options: Optional[dict] = None,
) -> None:
    """Publish a single message using a short lived publisher."""

    publisher_options = publisher_options or {}
    publish_options = publish_options or {}

    with KafkaPublisher(
        topic=topic,
        config=config,
        auto_flush=wait_for_delivery,
        **publisher_options,
    ) as publisher:
        publisher.publish(
            value,
            wait_for_delivery=wait_for_delivery,
            wait_timeout=5 if wait_for_delivery else 0,
            **publish_options,
        )


def consume_forever(
    topics: Sequence[str] | str,
    handler: MessageHandler,
    *,
    config: Optional[KafkaConfig] = None,
    poll_timeout: float = 1.0,
    subscriber_options: Optional[dict] = None,
    listen_options: Optional[dict] = None,
) -> None:
    """Create a subscriber, listen for messages, and block forever."""

    subscriber_options = subscriber_options or {}
    listen_options = listen_options or {}

    with KafkaSubscriber(topics=topics, config=config, **subscriber_options) as subscriber:
        subscriber.listen(handler, poll_timeout=poll_timeout, **listen_options)
