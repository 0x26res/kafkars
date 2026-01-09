# Kafkars Architecture

## Overview

Kafkars is a Rust-based Kafka consumer library with Python bindings that provides:

- **Ordered message delivery**: Messages are released in timestamp order across all partitions
- **Offset resolution**: Supports multiple offset policies (earliest, latest, committed, time-based)
- **Backpressure management**: Pauses fast partitions to prevent memory overflow

## Core Components

```text
┌─────────────────────────────────────────────────────────────────────────────┐
│                              ConsumerManager                                │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌──────────────┐    ┌──────────────────┐    ┌────────────────────────┐     │
│  │ BaseConsumer │    │  StartOffsets    │    │    PartitionInfo       │     │
│  │   (rdkafka)  │    │  (immutable)     │    │    (per partition)     │     │
│  └──────────────┘    └──────────────────┘    └────────────────────────┘     │
│         │                    │                         │                    │
│         │            ┌───────┴───────┐         ┌───────┴───────┐            │
│         │            │ start_offset  │         │ current_offset│            │
│         │            │ end_offset    │         │ timestamp_ms  │            │
│         │            │ (per partition)│        │ is_live       │            │
│         │            └───────────────┘         └───────────────┘            │
│         ▼                                                                   │
│  ┌──────────────────────────────────────────────────────────────────┐       │
│  │                      held_messages: Vec<TimestampedMessage>      │       │
│  │                         (sorted by timestamp_ms)                 │       │
│  └──────────────────────────────────────────────────────────────────┘       │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Message Flow

```mermaid
flowchart TD
    subgraph Kafka
        K1[Topic 1 / Partition 0]
        K2[Topic 2 / Partition 0]
    end

    subgraph ConsumerManager
        BC[BaseConsumer]
        HM[held_messages<br/>Vec sorted by timestamp]
        PI[partition_info<br/>HashMap]
        LWM[low_water_mark_ms]
    end

    subgraph Output
        BATCH[Released Batch<br/>≤ batch_size messages]
    end

    K1 --> BC
    K2 --> BC
    BC -->|poll| HM
    HM -->|update| PI
    PI -->|min timestamp of<br/>non-live partitions| LWM
    LWM -->|release messages<br/>where ts ≤ limit| BATCH
```

## Offset Resolution at Startup

When `ConsumerManager::create()` is called, offsets are resolved synchronously for all topic/partitions before any messages are consumed:

```mermaid
sequenceDiagram
    participant User
    participant CM as ConsumerManager
    participant Kafka

    User->>CM: create(config, topics, cutoff_ms, batch_size)

    loop For each topic
        CM->>Kafka: fetch_metadata(topic)
        Kafka-->>CM: partitions list
    end

    loop For each partition
        CM->>Kafka: fetch_watermarks(topic, partition)
        Kafka-->>CM: (low, high)

        alt Policy: Earliest
            CM->>CM: start_offset = low
        else Policy: Latest
            CM->>CM: start_offset = high
        else Policy: Committed
            CM->>Kafka: committed_offsets(topic, partition)
            Kafka-->>CM: committed offset
        else Policy: RelativeTime/AbsoluteTime
            CM->>Kafka: offsets_for_times(timestamp)
            Kafka-->>CM: offset at timestamp
        end

        CM->>CM: Store StartOffsets(start, end=high)
    end

    CM->>Kafka: assign(TopicPartitionList)
    CM-->>User: ConsumerManager ready
```

## Data Structures

### StartOffsets (Immutable)

Captured at creation time and never modified:

```rust
struct PartitionStartOffset {
    topic: String,
    partition: i32,
    start_offset: i64,   // Where we started consuming
    end_offset: i64,     // High watermark at creation time
}
```

### PartitionInfo (Mutable)

Updated as messages are consumed:

```rust
struct PartitionInfo {
    topic: String,
    partition: i32,
    current_offset: i64,     // Last consumed offset
    timestamp_ms: Option<i64>, // Last message timestamp
    is_live: bool,           // True when caught up to end_offset
}
```

### TimestampedMessage

Messages held in memory before release:

```rust
struct TimestampedMessage {
    key: Option<Vec<u8>>,
    value: Option<Vec<u8>>,
    topic: String,
    partition: i32,
    offset: i64,
    timestamp_ms: i64,
}
```

## Poll Cycle

```mermaid
flowchart TD
    START([poll called]) --> POLL1[Poll with timeout]
    POLL1 --> MSG1{Message<br/>received?}

    MSG1 -->|No| HOUSEKEEPING
    MSG1 -->|Yes| UPDATE1[Update partition_info]

    UPDATE1 --> PUSH[Push to held_messages]
    PUSH --> CHECK{held_messages.len<br/>< max_held?}

    CHECK -->|Yes| POLL0[Poll with zero timeout]
    POLL0 --> MSG0{Message?}
    MSG0 -->|Yes| UPDATE1
    MSG0 -->|No| SORT

    CHECK -->|No| SORT[Sort by timestamp_ms]

    SORT --> HOUSEKEEPING[Update low_water_mark<br/>Manage paused partitions]

    HOUSEKEEPING --> RELEASE[Release messages<br/>where ts ≤ limit<br/>up to batch_size]

    RELEASE --> RETURN([Return batch])
```

## Watermark-Based Release

The release mechanism ensures messages are returned in timestamp order:

```text
Timeline →

Partition 1:  [msg@100] [msg@200] [msg@300] [msg@400]  ← is_live=false (ts=400)
Partition 2:  [msg@150] [msg@250]                      ← is_live=false (ts=250)
                                   ↑
                            low_water_mark = 250

held_messages (sorted): [100, 150, 200, 250, 300, 400]
                         ↑_______________↑
                         Released (ts ≤ 250)
```

### Release Conditions

A message is released when:

1. `timestamp_ms <= low_water_mark_ms` (or `cutoff_ms` if all partitions are live)
2. `released.len() < batch_size`

### Partition Liveness

A partition becomes **live** when either:

1. Message timestamp >= `cutoff_ms`, OR
2. Current offset reaches the `end_offset` captured at creation time

```rust
info.is_live = msg.timestamp_ms >= self.cutoff_ms
            || (msg.offset + 1) >= end_offset;
```

## Backpressure Management

When `held_messages` exceeds `max_held_messages` (default: `batch_size * 5`):

```mermaid
flowchart LR
    subgraph Normal
        A[All partitions<br/>consuming]
    end

    subgraph Backpressure
        B[Fast partitions<br/>PAUSED]
        C[Slow partition<br/>continues]
    end

    subgraph Recovery
        D[All partitions<br/>RESUMED]
    end

    Normal -->|held > max| Backpressure
    Backpressure -->|held < batch_size| Recovery
    Recovery --> Normal
```

Partitions with timestamps ahead of the low water mark are paused to let slower partitions catch up.

## Python Interface

```python
from kafkars import ConsumerManager, SourceTopic

# Create source topics with offset policies
topics = [
    SourceTopic.from_earliest("events"),
    SourceTopic.from_relative_time("metrics", 3600000),  # 1 hour ago
]

# Create consumer manager
manager = ConsumerManager(
    config={"bootstrap.servers": "localhost:9092", "group.id": "my-group"},
    topics=topics,
    cutoff_ms=int(time.time() * 1000),  # Now
    batch_size=1000,
)

# Poll returns PyArrow RecordBatch
while True:
    batch = manager.poll(timeout_ms=1000)
    if batch.num_rows > 0:
        df = batch.to_pandas()
        process(df)

    if manager.is_live():
        break  # All partitions caught up
```

## File Structure

```text
src/
├── lib.rs              # Python bindings (PyO3)
├── consumer_manager.rs # Core consumer logic
└── source_topic.rs     # SourceTopic and OffsetPolicy types

bench/
├── poll_messages.py    # CLI tool for testing
└── README.md           # Usage instructions
```
