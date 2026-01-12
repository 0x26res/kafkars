# Batch Processing

kafkars is designed for high-throughput batch processing. This guide explains how to get the most out of it.

## How Batching Works

When you call `poll()`, kafkars:

1. Polls Kafka for available messages
2. Accumulates messages in an internal buffer
3. Sorts messages by timestamp across all partitions
4. Returns up to `batch_size` messages as a PyArrow RecordBatch

```python
manager = ConsumerManager(
    config={...},
    topics=[...],
    cutoff_ms=cutoff,
    batch_size=10_000,  # Maximum messages per poll
)
```

## Timestamp Ordering

Messages are returned in **timestamp order across all partitions**. This is achieved through:

1. **Low water mark tracking**: The minimum timestamp among non-live partitions
2. **Buffering**: Messages are held until it's safe to release them in order
3. **Backpressure**: Fast partitions are paused to prevent memory overflow

```text
Timeline →

Partition 1:  [msg@100] [msg@200] [msg@300] [msg@400]
Partition 2:  [msg@150] [msg@250]
                                   ↑
                            low_water_mark = 250

Output: [100, 150, 200, 250] (in order)
```

## Replay vs Live Mode

### Replay Mode

During replay (catching up to real-time):

- Messages are released based on the low water mark
- Timestamp ordering is guaranteed
- Throughput may be limited by the slowest partition

### Live Mode

Once all partitions are "live" (caught up):

- All buffered messages are released immediately
- New messages are returned as they arrive
- Check with `manager.is_live()`

```python
while True:
    batch = manager.poll(timeout_ms=1000)
    process(batch)

    if manager.is_live():
        print("Now in live mode!")
```

## Monitoring Progress

### Partition State

Get detailed information about each partition:

```python
state = manager.partition_state()
df = state.to_pandas()
print(df.to_markdown())
```

Returns:

| Column                    | Description                                |
|---------------------------|--------------------------------------------|
| `topic`                   | Topic name                                 |
| `partition`               | Partition number                           |
| `replay_start_offset`     | Starting offset (resolved at creation)     |
| `replay_end_offset`       | End offset (captured at creation)          |
| `consumed_offset`         | Last consumed offset                       |
| `released_offset`         | Last released offset                       |
| `last_consumed_timestamp` | Timestamp of last consumed message         |
| `is_live`                 | Whether partition has caught up            |
| `is_paused`               | Whether partition is paused (backpressure) |

### Other Metrics

```python
# Check if all partitions are live
manager.is_live()

# Number of messages buffered but not yet released
manager.held_message_count()

# Number of partitions currently paused
manager.paused_partition_count()

# Get the priming watermark (None if live)
manager.get_priming_watermark()
```

## Performance Tips

### 1. Use larger batch sizes

Larger batches reduce per-message overhead:

```python
# For high-throughput scenarios
manager = ConsumerManager(..., batch_size=50_000)
```

### 2. Process batches efficiently

Use vectorized operations instead of row-by-row processing:

```python
# Good: Vectorized
df = batch.to_pandas()
df['value_decoded'] = df['value'].str.decode('utf-8')

# Bad: Row-by-row
for i, row in df.iterrows():
    row['value'].decode('utf-8')
```

### 3. Use polars for better performance

Polars often outperforms pandas for batch processing:

```python
import polars as pl

batch = manager.poll(timeout_ms=1000)
df = pl.from_arrow(batch)
```
