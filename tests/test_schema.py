"""Tests to verify Python schema literals match Rust schema definitions."""

import kafkars


def test_message_schema_matches_rust():
    """Verify MESSAGE_SCHEMA matches the schema returned from Rust."""
    rust_schema = kafkars.get_message_schema()
    python_schema = kafkars.MESSAGE_SCHEMA

    assert rust_schema.equals(python_schema), (
        f"MESSAGE_SCHEMA mismatch:\n  Rust:   {rust_schema}\n  Python: {python_schema}"
    )


def test_partition_state_schema_matches_rust():
    """Verify PARTITION_STATE_SCHEMA matches the schema returned from Rust."""
    rust_schema = kafkars.get_partition_state_schema()
    python_schema = kafkars.PARTITION_STATE_SCHEMA

    assert rust_schema.equals(python_schema), (
        f"PARTITION_STATE_SCHEMA mismatch:\n"
        f"  Rust:   {rust_schema}\n"
        f"  Python: {python_schema}"
    )


def test_message_schema_fields():
    """Verify MESSAGE_SCHEMA has expected fields."""
    schema = kafkars.MESSAGE_SCHEMA
    field_names = [f.name for f in schema]

    assert field_names == ["key", "value", "topic", "partition", "offset", "timestamp"]


def test_partition_state_schema_fields():
    """Verify PARTITION_STATE_SCHEMA has expected fields."""
    schema = kafkars.PARTITION_STATE_SCHEMA
    field_names = [f.name for f in schema]

    assert field_names == [
        "topic",
        "partition",
        "replay_start_offset",
        "replay_end_offset",
        "consumed_offset",
        "released_offset",
        "last_message_timestamp",
        "cutoff",
        "is_live",
        "is_paused",
    ]
