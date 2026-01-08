# kafkars

[![PyPI](https://img.shields.io/pypi/v/kafkars.svg)](https://pypi.org/project/kafkars/)
[![GitHub stars](https://img.shields.io/github/stars/0x26res/kafkars?style=social)](https://github.com/0x26res/kafkars)
[![Python 3.10+](https://img.shields.io/badge/python-3.10%2B-blue.svg)](https://www.python.org/downloads/)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-%23000000.svg?logo=rust&logoColor=white)](https://www.rust-lang.org/)
[![Apache Arrow](https://img.shields.io/badge/Apache%20Arrow-powered-orange.svg)](https://arrow.apache.org/)

Rust-based, Arrow-powered Python Kafka client for high-throughput data pipelines.

## Motivation

Python's Global Interpreter Lock (GIL) and memory management create bottlenecks when consuming high-volume Kafka streams.
Traditional Python Kafka clients process messages one at a time,
requiring serialization/deserialization overhead for each message and limiting throughput.

**kafkars** solves this by:

- **Rust core**: All Kafka operations (polling, buffering, ordering) happen in Rust, bypassing the GIL
- **Batch processing**: Messages are accumulated and returned as Apache Arrow RecordBatches, not individual Python objects
- **Zero-copy where possible**: Arrow's columnar format enables efficient data transfer between Rust and Python
- **Vectorized operations**: Process thousands of messages at once with pandas, polars, or any Arrow-compatible library

This architecture is ideal for:

- Real-time analytics pipelines
- ML feature stores consuming from Kafka
- High-volume event processing
- Data lake ingestion

## Features

- **Ordered delivery**: Messages released in timestamp order across all partitions
- **Flexible offset policies**: Start from earliest, latest, committed, or any timestamp
- **Backpressure management**: Automatically pauses fast partitions to prevent memory overflow
- **Arrow-native output**: Returns PyArrow RecordBatch for efficient downstream processing

## Installation

```bash
pip install kafkars
```

## Quick Start

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

## Message Schema

Each poll returns a RecordBatch with the following schema:

| Column      | Type                 | Description                |
|-------------|----------------------|----------------------------|
| `key`       | `binary`             | Message key (nullable)     |
| `value`     | `binary`             | Message payload (nullable) |
| `topic`     | `utf8`               | Source topic name          |
| `partition` | `int32`              | Partition number           |
| `offset`    | `int64`              | Message offset             |
| `timestamp` | `timestamp[ms, UTC]` | Message timestamp          |

## Offset Policies

| Policy                          | Description                            |
|---------------------------------|----------------------------------------|
| `from_earliest(topic)`          | Start from the beginning               |
| `from_latest(topic)`            | Start from the end (new messages only) |
| `from_committed(topic)`         | Resume from last committed offset      |
| `from_relative_time(topic, ms)` | Start from N milliseconds ago          |
| `from_absolute_time(topic, ms)` | Start from specific Unix timestamp     |

## Architecture

See [docs/architecture.md](docs/architecture.md) for detailed design documentation.

## License

Apache 2.0
