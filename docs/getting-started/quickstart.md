# Quick Start

This guide will help you get up and running with kafkars in minutes.

## Basic Usage

### 1. Import the library

```python
import time
from kafkars import ConsumerManager, SourceTopic
```

### 2. Define your topics

Specify which topics to consume and the offset policy for each:

```python
topics = [
    SourceTopic.from_earliest("my-topic"),  # Start from beginning
]
```

### 3. Create a consumer

```python
manager = ConsumerManager(
    config={
        "bootstrap.servers": "localhost:9092",
        "group.id": "my-consumer-group",
    },
    topics=topics,
    cutoff_ms=int(time.time() * 1000),  # Current time
    batch_size=1000,
)
```

### 4. Poll for messages

```python
while True:
    batch = manager.poll(timeout_ms=1000)

    if batch.num_rows > 0:
        # batch is a PyArrow RecordBatch
        df = batch.to_pandas()
        process_messages(df)

    if manager.is_live():
        print("Caught up to real-time!")
        break
```

## Message Schema

Each batch contains the following columns:

| Column      | Type                 | Description                |
|-------------|----------------------|----------------------------|
| `key`       | `binary`             | Message key (nullable)     |
| `value`     | `binary`             | Message payload (nullable) |
| `topic`     | `utf8`               | Source topic name          |
| `partition` | `int32`              | Partition number           |
| `offset`    | `int64`              | Message offset             |
| `timestamp` | `timestamp[ms, UTC]` | Message timestamp          |

## Processing with pandas

```python
batch = manager.poll(timeout_ms=1000)
df = batch.to_pandas()

# Decode string values
df['value_str'] = df['value'].apply(lambda x: x.decode('utf-8') if x else None)

# Filter by topic
events = df[df['topic'] == 'events']
```

## Processing with polars

```python
import polars as pl

batch = manager.poll(timeout_ms=1000)
df = pl.from_arrow(batch)

# Process with polars
df = df.with_columns(
    pl.col('value').cast(pl.Utf8).alias('value_str')
)
```

## Next Steps

- Learn about [Offset Policies](../guide/offset-policies.md) for flexible replay
- Understand [Batch Processing](../guide/batch-processing.md) for optimal throughput
