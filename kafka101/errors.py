"""Custom exceptions for the kafka101 package."""

from __future__ import annotations

from typing import Optional


class KafkaClientError(RuntimeError):
    """Base exception raised by the kafka101 helpers."""


class KafkaConfigurationError(KafkaClientError):
    """Raised when a configuration value is missing or invalid."""


class KafkaDeliveryError(KafkaClientError):
    """Raised when the producer fails to deliver a message."""

    def __init__(self, message: str, *, error: Optional[Exception] = None) -> None:
        super().__init__(message)
        self.error = error


class KafkaConsumeError(KafkaClientError):
    """Raised when consuming from Kafka fails in a non recoverable way."""

    def __init__(self, message: str, *, error: Optional[Exception] = None) -> None:
        super().__init__(message)
        self.error = error
