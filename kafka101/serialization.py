"""Serialization helpers for kafka101."""

from __future__ import annotations

import json
from typing import Any, Callable, Optional

JsonDumps = Callable[[Any], str]


class JsonSerializer:
    """JSON serializer that tolerates primitive Python types."""

    def __init__(
        self,
        *,
        ensure_ascii: bool = False,
        default: Optional[JsonDumps] = None,
        strict: bool = False,
    ) -> None:
        self._ensure_ascii = ensure_ascii
        self._default = default
        self._strict = strict

    def serialize(self, value: Any) -> Optional[bytes]:
        """Serialize the payload to UTF-8 encoded JSON."""
        if value is None:
            return None
        if isinstance(value, bytes):
            return value
        if isinstance(value, bytearray):
            return bytes(value)
        if isinstance(value, str):
            return value.encode("utf-8")
        return json.dumps(
            value,
            ensure_ascii=self._ensure_ascii,
            default=self._default,
        ).encode("utf-8")

    def deserialize(self, value: Optional[bytes]) -> Any:
        """Deserialize a UTF-8 encoded JSON payload."""
        if value is None:
            return None
        if not value:
            return None
        text = value.decode("utf-8")
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            if self._strict:
                raise
            return text


def default_key_serializer(value: Optional[Any]) -> Optional[bytes]:
    """Serialize keys with minimal assumptions."""
    if value is None:
        return None
    if isinstance(value, bytes):
        return value
    if isinstance(value, bytearray):
        return bytes(value)
    return str(value).encode("utf-8")


def default_key_deserializer(value: Optional[bytes]) -> Optional[str]:
    """Deserialize keys to strings."""
    if value is None:
        return None
    if not value:
        return ""
    return value.decode("utf-8")
