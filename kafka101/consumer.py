"""Kafka consumer utilities built on top of confluent_kafka."""

from __future__ import annotations

import logging
import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple, Union

from confluent_kafka import Consumer, KafkaError, KafkaException, Message

from .config import KafkaConfig
from .errors import KafkaClientError, KafkaConsumeError
from .serialization import JsonSerializer, default_key_deserializer

MessageHandler = Callable[["KafkaRecord"], None]
ErrorHandler = Callable[[Exception], None]


@dataclass
class KafkaRecord:
    """Structured view of a Kafka message."""

    topic: str
    partition: int
    offset: int
    key: Any
    value: Any
    headers: Dict[str, Any]
    timestamp: Optional[datetime]
    raw: Message

    def header(self, name: str, default: Any = None) -> Any:
        return self.headers.get(name, default)


class KafkaSubscriber:
    """High level wrapper around confluent_kafka.Consumer."""

    def __init__(
        self,
        topics: Union[str, Sequence[str]],
        *,
        config: Optional[KafkaConfig] = None,
        group_id: Optional[str] = None,
        enable_auto_commit: Optional[bool] = None,
        value_deserializer: Optional[Callable[[Optional[bytes]], Any]] = None,
        key_deserializer: Optional[Callable[[Optional[bytes]], Any]] = None,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        self._topics = [topics] if isinstance(topics, str) else list(topics)
        if not self._topics:
            raise KafkaClientError("At least one topic must be supplied")

        self._config = config or KafkaConfig()
        self._auto_commit = self._config.enable_auto_commit if enable_auto_commit is None else enable_auto_commit
        self._value_deserializer = value_deserializer or JsonSerializer().deserialize
        self._key_deserializer = key_deserializer or default_key_deserializer
        self._logger = logger or logging.getLogger(__name__)
        self._consumer = self._create_consumer(group_id=group_id, enable_auto_commit=self._auto_commit)
        self._stop_event = threading.Event()

    def _create_consumer(self, *, group_id: Optional[str], enable_auto_commit: bool) -> Consumer:
        try:
            consumer = Consumer(
                self._config.consumer_config(
                    group_id=group_id,
                    enable_auto_commit=enable_auto_commit,
                )
            )
        except Exception as exc:  # pragma: no cover - pass-through to caller
            raise KafkaClientError("Could not create Kafka consumer") from exc

        try:
            consumer.subscribe(self._topics)
        except KafkaException as exc:
            raise KafkaClientError(f"Failed to subscribe to topics: {self._topics}") from exc
        return consumer

    def poll(self, timeout: float = 1.0) -> Optional[KafkaRecord]:
        """Poll Kafka and return a structured record or None."""
        if self._consumer is None:
            raise KafkaClientError("Consumer is not initialised")

        message = self._consumer.poll(timeout=timeout)
        if message is None:
            return None
        if message.error():
            if message.error().code() == KafkaError._PARTITION_EOF:
                self._logger.debug(
                    "Reached end of partition %s[%s] at offset %s",
                    message.topic(),
                    message.partition(),
                    message.offset(),
                )
                return None
            raise KafkaConsumeError(str(message.error()), error=message.error())
        return self._to_record(message)

    def listen(
        self,
        handler: MessageHandler,
        *,
        poll_timeout: float = 1.0,
        commit_on_success: bool = False,
        stop_event: Optional[threading.Event] = None,
        on_error: Optional[ErrorHandler] = None,
        raise_on_error: bool = False,
    ) -> None:
        """Continuously poll Kafka and dispatch messages to the handler."""
        if self._consumer is None:
            raise KafkaClientError("Consumer is not initialised")

        self._stop_event.clear()
        while not self._stop_event.is_set():
            if stop_event and stop_event.is_set():
                break
            try:
                record = self.poll(timeout=poll_timeout)
            except KafkaConsumeError as exc:
                if on_error:
                    on_error(exc)
                elif raise_on_error:
                    raise
                else:
                    self._logger.error("Kafka consume error: %s", exc)
                continue

            if record is None:
                continue

            try:
                handler(record)
                if commit_on_success and not self._auto_commit:
                    self._consumer.commit(record.raw, asynchronous=False)
            except Exception as exc:
                self._logger.exception("Message handler raised an exception")
                if on_error:
                    on_error(exc)
                if raise_on_error:
                    raise

    def stop(self) -> None:
        """Signal the consumer loop to stop."""
        self._stop_event.set()

    def commit(self, record: KafkaRecord, *, asynchronous: bool = False) -> None:
        """Commit the offset of the provided record."""
        if self._consumer is None:
            raise KafkaClientError("Consumer is not initialised")
        self._consumer.commit(record.raw, asynchronous=asynchronous)

    def pause(self) -> None:
        """Pause consumption across all assigned partitions."""
        if self._consumer is None:
            raise KafkaClientError("Consumer is not initialised")
        assignments = self._consumer.assignment()
        if not assignments:
            return
        self._consumer.pause(assignments)

    def resume(self) -> None:
        """Resume consumption across all assigned partitions."""
        if self._consumer is None:
            raise KafkaClientError("Consumer is not initialised")
        assignments = self._consumer.assignment()
        if not assignments:
            return
        self._consumer.resume(assignments)

    def close(self) -> None:
        """Close the consumer and release resources."""
        if self._consumer is None:
            return
        try:
            self._consumer.close()
        finally:
            self._consumer = None
            self._stop_event.set()

    def __enter__(self) -> "KafkaSubscriber":
        return self

    def __exit__(self, exc_type, exc, exc_tb) -> None:
        self.close()

    def _to_record(self, message: Message) -> KafkaRecord:
        return KafkaRecord(
            topic=message.topic(),
            partition=message.partition(),
            offset=message.offset(),
            key=self._key_deserializer(message.key()),
            value=self._value_deserializer(message.value()),
            headers=self._parse_headers(message.headers()),
            timestamp=self._parse_timestamp(message.timestamp()),
            raw=message,
        )

    def _parse_headers(self, headers: Optional[List[Tuple[str, Any]]]) -> Dict[str, Any]:
        if not headers:
            return {}
        parsed: Dict[str, Any] = {}
        for key, value in headers:
            if value is None:
                parsed[key] = None
            elif isinstance(value, bytes):
                try:
                    parsed[key] = value.decode("utf-8")
                except UnicodeDecodeError:
                    parsed[key] = value
            else:
                parsed[key] = value
        return parsed

    def _parse_timestamp(self, timestamp: Optional[tuple]) -> Optional[datetime]:
        if not timestamp:
            return None
        ts_type, ts_value = timestamp
        if ts_type != 1 or ts_value is None:
            return None
        return datetime.fromtimestamp(ts_value / 1000, tz=timezone.utc)


KafkaConsumer = KafkaSubscriber
