# kafkars

Rust-based, Arrow-powered Python Kafka client for high-throughput data pipelines.

## Why kafkars?

Python's Global Interpreter Lock (GIL) and memory management create bottlenecks when consuming high-volume Kafka streams. Traditional Python Kafka clients process messages one at a time, requiring serialization/deserialization overhead for each message and limiting throughput.

**kafkars** solves this by:

- **Rust core**: All Kafka operations (polling, buffering, ordering) happen in Rust, bypassing the GIL
- **Batch processing**: Messages are accumulated and returned as Apache Arrow RecordBatches, not individual Python objects
- **Zero-copy where possible**: Arrow's columnar format enables efficient data transfer between Rust and Python
- **Vectorized operations**: Process thousands of messages at once with pandas, polars, or any Arrow-compatible library

## Use Cases

This architecture is ideal for:

- Real-time analytics pipelines
- ML feature stores consuming from Kafka
- High-volume event processing
- Data lake ingestion

## Important: Analytics-Focused Design

**kafkars does not commit offsets.** It is designed for analytics and high-throughput batch processing, not transactional workloads.

- **No exactly-once semantics**: Messages may be reprocessed if your application restarts
- **No offset tracking**: You control where to start reading via offset policies
- **Stateless consumers**: Each consumer instance starts fresh based on the configured policy

If you need exactly-once processing, transactional guarantees, or automatic offset management, use a traditional Kafka client like `confluent-kafka-python`.

## Features

- **Ordered delivery**: Messages released in timestamp order across all partitions
- **Flexible offset policies**: Start from earliest, latest, or any timestamp
- **Backpressure management**: Automatically pauses fast partitions to prevent memory overflow
- **Arrow-native output**: Returns PyArrow RecordBatch for efficient downstream processing

## Quick Example

```python
import time
from kafkars import ConsumerManager, SourceTopic

# Define source topics with offset policies
topics = [
    SourceTopic.from_earliest("events"),
    SourceTopic.from_relative_time("metrics", 3600_000),  # 1 hour ago
]

# Create consumer
manager = ConsumerManager(
    config={
        "bootstrap.servers": "localhost:9092",
        "group.id": "my-consumer-group",
    },
    topics=topics,
    cutoff_ms=int(time.time() * 1000),
    batch_size=10_000,
)

# Poll returns PyArrow RecordBatch
while True:
    batch = manager.poll(timeout_ms=1000)
    if batch.num_rows > 0:
        # Convert to pandas/polars for processing
        df = batch.to_pandas()
        print(f"Received {len(df)} messages")

    if manager.is_live():
        print("Caught up to real-time")
        break
```

## Getting Started

Head over to the [Installation](getting-started/installation.md) guide to get started.
