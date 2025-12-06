"""Configuration helpers for kafka101."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Any, Dict, Optional

from .errors import KafkaConfigurationError


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "y", "yes", "on"}


def _env_int(name: str) -> Optional[int]:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return None
    try:
        return int(raw)
    except ValueError as exc:
        raise KafkaConfigurationError(f"Environment variable {name} must be an integer") from exc


@dataclass
class KafkaConfig:
    """Configuration builder for kafka101 producer and consumer helpers."""

    bootstrap_servers: str = field(default_factory=lambda: os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    security_protocol: str = field(default_factory=lambda: os.getenv("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT"))
    sasl_mechanism: Optional[str] = field(default_factory=lambda: os.getenv("KAFKA_SASL_MECHANISM"))
    sasl_username: Optional[str] = field(default_factory=lambda: os.getenv("KAFKA_SASL_USERNAME"))
    sasl_password: Optional[str] = field(default_factory=lambda: os.getenv("KAFKA_SASL_PASSWORD"))
    ssl_ca_location: Optional[str] = field(default_factory=lambda: os.getenv("KAFKA_SSL_CA_LOCATION"))
    client_id: Optional[str] = field(default_factory=lambda: os.getenv("KAFKA_CLIENT_ID"))
    group_id: str = field(default_factory=lambda: os.getenv("KAFKA_GROUP_ID", "default-service"))
    auto_offset_reset: str = field(default_factory=lambda: os.getenv("KAFKA_AUTO_OFFSET_RESET", "latest"))
    enable_auto_commit: bool = field(default_factory=lambda: _env_bool("KAFKA_ENABLE_AUTO_COMMIT", True))
    session_timeout_ms: Optional[int] = field(default_factory=lambda: _env_int("KAFKA_SESSION_TIMEOUT_MS"))
    request_timeout_ms: Optional[int] = field(default_factory=lambda: _env_int("KAFKA_REQUEST_TIMEOUT_MS"))
    extra_producer_config: Dict[str, Any] = field(default_factory=dict)
    extra_consumer_config: Dict[str, Any] = field(default_factory=dict)

    def producer_config(self) -> Dict[str, Any]:
        config: Dict[str, Any] = {"bootstrap.servers": self.bootstrap_servers}
        if self.client_id:
            config["client.id"] = self.client_id
        if self.request_timeout_ms is not None:
            config["request.timeout.ms"] = self.request_timeout_ms
        config.update(self._security_config())
        config.update(self.extra_producer_config)
        return config

    def consumer_config(
        self,
        *,
        group_id: Optional[str] = None,
        enable_auto_commit: Optional[bool] = None,
    ) -> Dict[str, Any]:
        config: Dict[str, Any] = {
            "bootstrap.servers": self.bootstrap_servers,
            "group.id": group_id or self.group_id,
            "auto.offset.reset": self.auto_offset_reset,
            "enable.auto.commit": self.enable_auto_commit if enable_auto_commit is None else enable_auto_commit,
        }
        if self.client_id:
            config["client.id"] = self.client_id
        if self.session_timeout_ms is not None:
            config["session.timeout.ms"] = self.session_timeout_ms
        if self.request_timeout_ms is not None:
            config["request.timeout.ms"] = self.request_timeout_ms
        config.update(self._security_config())
        config.update(self.extra_consumer_config)
        return config

    def _security_config(self) -> Dict[str, Any]:
        protocol = self.security_protocol.upper() if self.security_protocol else "PLAINTEXT"
        config: Dict[str, Any] = {"security.protocol": protocol}

        if protocol.startswith("SASL"):
            if not self.sasl_mechanism:
                raise KafkaConfigurationError("SASL mechanism must be provided when using SASL security protocol")
            if not self.sasl_username or not self.sasl_password:
                raise KafkaConfigurationError("SASL credentials must be provided when using SASL security protocol")
            config["sasl.mechanism"] = self.sasl_mechanism
            config["sasl.username"] = self.sasl_username
            config["sasl.password"] = self.sasl_password

        if protocol.endswith("SSL") and self.ssl_ca_location:
            config["ssl.ca.location"] = self.ssl_ca_location

        return config
