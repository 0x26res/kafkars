"""PyArrow schemas for Kafkars data structures."""

import pyarrow as pa

MESSAGE_SCHEMA = pa.schema(
    [
        pa.field("key", pa.binary(), nullable=True),
        pa.field("value", pa.binary(), nullable=True),
        pa.field("topic", pa.utf8(), nullable=False),
        pa.field("partition", pa.int32(), nullable=False),
        pa.field("offset", pa.int64(), nullable=False),
        pa.field("timestamp", pa.timestamp("ms", tz="UTC"), nullable=False),
    ]
)

PARTITION_STATE_SCHEMA = pa.schema(
    [
        pa.field("topic", pa.utf8(), nullable=False),
        pa.field("partition", pa.int32(), nullable=False),
        pa.field("start_offset", pa.int64(), nullable=False),
        pa.field("end_offset", pa.int64(), nullable=False),
        pa.field("current_offset", pa.int64(), nullable=False),
        pa.field("last_message_timestamp", pa.timestamp("ms", tz="UTC"), nullable=True),
        pa.field("cutoff", pa.timestamp("ms", tz="UTC"), nullable=False),
        pa.field("is_live", pa.bool_(), nullable=False),
    ]
)
