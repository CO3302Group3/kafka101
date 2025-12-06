"""Kafka publisher utilities built on top of confluent_kafka."""

from __future__ import annotations

import logging
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple, Union

from confluent_kafka import KafkaException, Producer

from .config import KafkaConfig
from .errors import KafkaClientError, KafkaDeliveryError
from .serialization import JsonSerializer, default_key_serializer

DeliveryCallback = Callable[[Any, Any], None]
HeadersInput = Optional[Union[Mapping[str, Any], Sequence[Tuple[str, Any]]]]


class KafkaPublisher:
    """High level wrapper around confluent_kafka.Producer."""

    def __init__(
        self,
        topic: str,
        *,
        config: Optional[KafkaConfig] = None,
        value_serializer: Optional[Callable[[Any], Optional[bytes]]] = None,
        key_serializer: Optional[Callable[[Any], Optional[bytes]]] = None,
        auto_flush: bool = False,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        self.topic = topic
        self._config = config or KafkaConfig()
        self._value_serializer = value_serializer or JsonSerializer().serialize
        self._key_serializer = key_serializer or default_key_serializer
        self._auto_flush = auto_flush
        self._logger = logger or logging.getLogger(__name__)
        self._producer = self._create_producer()

    def _create_producer(self) -> Producer:
        try:
            return Producer(self._config.producer_config())
        except Exception as exc:  # pragma: no cover - pass-through to calling code
            raise KafkaClientError("Could not create Kafka producer") from exc

    def publish(
        self,
        value: Any,
        *,
        key: Any = None,
        headers: HeadersInput = None,
        partition: Optional[int] = None,
        on_delivery: Optional[DeliveryCallback] = None,
        wait_for_delivery: bool = False,
        wait_timeout: Optional[float] = None,
    ) -> None:
        """Publish a single message to Kafka."""
        if self._producer is None:
            raise KafkaClientError("Producer is not initialised")

        payload = self._value_serializer(value)
        key_payload = self._key_serializer(key) if self._key_serializer else key
        header_list = self._normalise_headers(headers)
        callback = self._wrap_delivery_callback(on_delivery)

        self._producer.poll(0)
        attempts = 0
        while True:
            try:
                if partition is None:
                    self._producer.produce(
                        topic=self.topic,
                        value=payload,
                        key=key_payload,
                        headers=header_list,
                        on_delivery=callback,
                    )
                else:
                    self._producer.produce(
                        topic=self.topic,
                        value=payload,
                        key=key_payload,
                        headers=header_list,
                        partition=partition,
                        on_delivery=callback,
                    )
                break
            except BufferError:
                attempts += 1
                self._logger.warning(
                    "Kafka producer buffer is full; backing off and retrying (attempt %s)",
                    attempts,
                )
                self._producer.poll(0.5)
            except KafkaException as exc:
                raise KafkaDeliveryError("Failed to queue message for delivery", error=exc) from exc

        if self._auto_flush or wait_for_delivery:
            self.flush(timeout=wait_timeout)

    def publish_batch(
        self,
        records: Iterable[Tuple[Any, Dict[str, Any]]],
        *,
        wait_for_delivery: bool = False,
        wait_timeout: Optional[float] = None,
    ) -> None:
        """Publish a batch of messages provided as (value, kwargs) tuples."""
        for value, kwargs in records:
            self.publish(value, **kwargs)
        if wait_for_delivery:
            self.flush(timeout=wait_timeout)

    def poll(self, timeout: float = 0.0) -> None:
        """Drive the producer event loop to trigger delivery callbacks."""
        if self._producer is None:
            raise KafkaClientError("Producer is not initialised")
        self._producer.poll(timeout)

    def flush(self, timeout: Optional[float] = None) -> None:
        """Flush pending messages and raise if any remain."""
        if self._producer is None:
            raise KafkaClientError("Producer is not initialised")
        if timeout is None:
            timeout = 0
        remaining = self._producer.flush(timeout)
        if remaining:
            raise KafkaDeliveryError(f"Failed to deliver {remaining} message(s)")

    def close(self, timeout: Optional[float] = None) -> None:
        """Flush any outstanding records and dispose the producer."""
        if self._producer is None:
            return
        try:
            self.flush(timeout=timeout)
        finally:
            self._producer = None

    def __enter__(self) -> "KafkaPublisher":
        return self

    def __exit__(self, exc_type, exc, exc_tb) -> None:
        self.close()

    def _wrap_delivery_callback(self, callback: Optional[DeliveryCallback]) -> Optional[DeliveryCallback]:
        if callback is None:
            return self._default_delivery_callback

        def wrapped(err: Any, msg: Any) -> None:
            if err is not None:
                self._logger.error("Kafka delivery failed: %s", err)
            callback(err, msg)

        return wrapped

    def _default_delivery_callback(self, err: Any, msg: Any) -> None:
        if err is not None:
            self._logger.error(
                "Kafka delivery failed for topic %s partition %s offset %s: %s",
                msg.topic() if msg else self.topic,
                msg.partition() if msg else "?",
                msg.offset() if msg else "?",
                err,
            )

    def _normalise_headers(self, headers: HeadersInput) -> Optional[List[Tuple[str, Optional[Union[str, bytes]]]]]:
        if headers is None:
            return None
        if isinstance(headers, Mapping):
            items = headers.items()
        else:
            items = headers
        normalised: List[Tuple[str, Optional[Union[str, bytes]]]] = []
        for key, value in items:
            if value is None:
                normalised.append((key, None))
            elif isinstance(value, (bytes, bytearray)):
                normalised.append((key, bytes(value)))
            else:
                normalised.append((key, str(value)))
        return normalised


KafkaProducer = KafkaPublisher
