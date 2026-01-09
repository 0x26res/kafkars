# API Reference

## ConsumerManager

The main class for consuming messages from Kafka.

### Constructor

```python
ConsumerManager(
    config: dict[str, str],
    topics: list[SourceTopic],
    cutoff_ms: int,
    batch_size: int,
)
```

**Parameters:**

- `config`: Kafka consumer configuration (e.g., `bootstrap.servers`, `group.id`)
- `topics`: List of `SourceTopic` objects defining topics and offset policies
- `cutoff_ms`: Unix timestamp (milliseconds) defining the boundary between replay and live mode
- `batch_size`: Maximum number of messages to return per `poll()` call

**Example:**

```python
from kafkars import ConsumerManager, SourceTopic

manager = ConsumerManager(
    config={
        "bootstrap.servers": "localhost:9092",
        "group.id": "my-group",
    },
    topics=[SourceTopic.from_earliest("events")],
    cutoff_ms=int(time.time() * 1000),
    batch_size=1000,
)
```

### Methods

#### poll

```python
def poll(timeout_ms: int) -> pyarrow.RecordBatch
```

Poll for messages from Kafka.

**Parameters:**

- `timeout_ms`: Maximum time to wait for messages (milliseconds)

**Returns:** PyArrow RecordBatch with message data

---

#### is_live

```python
def is_live() -> bool
```

Check if all partitions have caught up to their replay end offset or cutoff time.

---

#### partition_state

```python
def partition_state() -> pyarrow.RecordBatch
```

Get the current state of all partitions.

---

#### held_message_count

```python
def held_message_count() -> int
```

Get the number of messages currently buffered but not yet released.

---

#### paused_partition_count

```python
def paused_partition_count() -> int
```

Get the number of partitions currently paused due to backpressure.

---

#### get_priming_watermark

```python
def get_priming_watermark() -> int | None
```

Get the current low water mark timestamp, or `None` if all partitions are live.

---

## SourceTopic

Defines a topic with its offset policy.

### Factory Methods

#### from_earliest

```python
@staticmethod
def from_earliest(name: str) -> SourceTopic
```

Create a SourceTopic starting from the earliest available offset.

---

#### from_latest

```python
@staticmethod
def from_latest(name: str) -> SourceTopic
```

Create a SourceTopic starting from the latest offset (new messages only).

---

#### from_committed

```python
@staticmethod
def from_committed(name: str) -> SourceTopic
```

Create a SourceTopic starting from the last committed offset.

---

#### from_relative_time

```python
@staticmethod
def from_relative_time(name: str, time_ms: int) -> SourceTopic
```

Create a SourceTopic starting from a relative time offset.

**Parameters:**

- `name`: Topic name
- `time_ms`: Milliseconds before now

---

#### from_absolute_time

```python
@staticmethod
def from_absolute_time(name: str, time_ms: int) -> SourceTopic
```

Create a SourceTopic starting from an absolute Unix timestamp.

**Parameters:**

- `name`: Topic name
- `time_ms`: Unix timestamp in milliseconds

---

## Schemas

### MESSAGE_SCHEMA

PyArrow schema for message batches:

```python
from kafkars import MESSAGE_SCHEMA

# Fields:
# - key: binary (nullable)
# - value: binary (nullable)
# - topic: utf8
# - partition: int32
# - offset: int64
# - timestamp: timestamp[ms, tz=UTC]
```

### PARTITION_STATE_SCHEMA

PyArrow schema for partition state:

```python
from kafkars import PARTITION_STATE_SCHEMA

# Fields:
# - topic: utf8
# - partition: int32
# - replay_start_offset: int64
# - replay_end_offset: int64
# - consumed_offset: int64
# - released_offset: int64
# - last_message_timestamp: timestamp[ms, tz=UTC] (nullable)
# - cutoff: timestamp[ms, tz=UTC]
# - is_live: bool
# - is_paused: bool
```

---

## Utility Functions

### get_message_schema

```python
def get_message_schema() -> pyarrow.Schema
```

Get the message schema from the Rust library.

---

### get_partition_state_schema

```python
def get_partition_state_schema() -> pyarrow.Schema
```

Get the partition state schema from the Rust library.

---

### validate_source_topic

```python
def validate_source_topic(topic: SourceTopic) -> None
```

Validate a SourceTopic object. Raises an exception if invalid.
