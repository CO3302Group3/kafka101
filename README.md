# kafka101

High-level helpers for publishing and consuming Kafka messages with `confluent-kafka`. The package wraps the low-level producer and consumer clients to give microservices a consistent, opinionated interface that works well with JSON payloads and environment-driven configuration.

## Installation

```powershell
pip install -r requirements.txt
```

or include the dependency directly:

```powershell
pip install confluent-kafka>=2.3
```

## Quick start

### Publishing

```python
from kafka101 import KafkaPublisher

publisher = KafkaPublisher("alerts")
publisher.publish({"type": "PING", "payload": "hello"}, key="heartbeat", wait_for_delivery=True)
publisher.close()
```

Or use the convenience helper:

```python
from kafka101.utility import publish_message

publish_message("alerts", {"type": "PING"})
```

### Consuming

```python
from kafka101 import KafkaSubscriber

subscriber = KafkaSubscriber(["alerts"], group_id="alert-processor")

def handle(record):
    print(record.topic, record.value)

subscriber.listen(handle)
```

Using the blocking helper:

```python
from kafka101.utility import consume_forever

consume_forever("alerts", handle)
```

## Configuration

Configuration values are read from environment variables via `KafkaConfig`.

| Environment variable           | Description                                 | Default             |
| ------------------------------ | ------------------------------------------- | ------------------- |
| `KAFKA_BOOTSTRAP_SERVERS`      | Kafka broker list                            | `localhost:9092`    |
| `KAFKA_CLIENT_ID`              | Client identifier                            | unset               |
| `KAFKA_GROUP_ID`               | Consumer group                               | `default-service`   |
| `KAFKA_AUTO_OFFSET_RESET`      | `earliest` or `latest`                       | `latest`            |
| `KAFKA_ENABLE_AUTO_COMMIT`     | `true`/`false`                               | `true`              |
| `KAFKA_SECURITY_PROTOCOL`      | `PLAINTEXT`, `SASL_SSL`, etc.                | `PLAINTEXT`         |
| `KAFKA_SASL_MECHANISM`         | SASL mechanism when SASL is enabled          | unset               |
| `KAFKA_SASL_USERNAME`          | SASL username                                | unset               |
| `KAFKA_SASL_PASSWORD`          | SASL password                                | unset               |
| `KAFKA_SSL_CA_LOCATION`        | Path to CA certificate bundle for SSL        | unset               |
| `KAFKA_SESSION_TIMEOUT_MS`     | Consumer session timeout (ms)                | unset               |
| `KAFKA_REQUEST_TIMEOUT_MS`     | Request timeout (ms)                         | unset               |

You can also inject extra client configuration programmatically:

```python
from kafka101 import KafkaConfig, KafkaPublisher

config = KafkaConfig(extra_producer_config={"compression.type": "gzip"})
publisher = KafkaPublisher("alerts", config=config)
```

## Error handling

Custom exceptions are provided under `kafka101.errors`:

- `KafkaConfigurationError` for missing or invalid settings
- `KafkaDeliveryError` when a producer fails to deliver
- `KafkaConsumeError` for non-recoverable consumer errors

Consumers accept `on_error` and `raise_on_error` parameters, allowing handlers to surface issues without breaking the polling loop.

## Testing locally

1. Start a Kafka broker (for example via Docker or Redpanda).
2. Create a topic you plan to use.
3. Run publisher/consumer scripts, or use the provided helpers.

For integration tests, consider using [`testcontainers`](https://pypi.org/project/testcontainers/) to spin up ephemeral Kafka clusters.
