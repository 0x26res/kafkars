# Offset Policies

kafkars supports flexible offset policies that let you control where to start consuming from each topic.

## Available Policies

### Latest

Start from the end of the topic (new messages only):

```python
from kafkars import SourceTopic

topic = SourceTopic.from_latest("my-topic")
```

### Earliest

Start from the beginning of the topic:

```python
topic = SourceTopic.from_earliest("my-topic")
```

### Relative Time

Start from a specific time offset from now:

```python
# Start from 1 hour ago
topic = SourceTopic.from_relative_time("my-topic", 3600_000)  # milliseconds

# Start from 24 hours ago
topic = SourceTopic.from_relative_time("my-topic", 86400_000)
```

### Absolute Time

Start from a specific Unix timestamp:

```python
import datetime

# Start from a specific date/time
dt = datetime.datetime(2024, 1, 1, 0, 0, 0, tzinfo=datetime.timezone.utc)
timestamp_ms = int(dt.timestamp() * 1000)

topic = SourceTopic.from_absolute_time("my-topic", timestamp_ms)
```

## Multiple Topics with Different Policies

You can consume from multiple topics with different offset policies:

```python
from kafkars import ConsumerManager, SourceTopic

topics = [
    SourceTopic.from_earliest("historical-events"),
    SourceTopic.from_relative_time("recent-metrics", 3600_000),
    SourceTopic.from_latest("live-updates"),
]

manager = ConsumerManager(
    config={"bootstrap.servers": "localhost:9092", "group.id": "my-group"},
    topics=topics,
    cutoff_ms=int(time.time() * 1000),
    batch_size=1000,
)
```

## Offset Resolution

Offsets are resolved **synchronously at creation time**. This means:

1. When you create a `ConsumerManager`, it immediately queries Kafka for the starting offset of each partition
2. The resolved offsets are stored immutably
3. Consumption starts from these exact offsets

This ensures deterministic replay behavior - creating a consumer at the same time with the same policy will start from the same offset.
