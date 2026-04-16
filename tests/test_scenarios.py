"""Tests for JSON-driven scenarios using the testing framework."""

from pathlib import Path

import pytest

from kafkars.testing import run_scenario, run_scenario_file


SCENARIOS_DIR = Path(__file__).parent / "scenarios"


class TestBasicScenarios:
    """Test basic scenario execution."""

    def test_simple_single_message(self):
        """Test the simplest possible scenario: one message in, one message out."""
        result = run_scenario(
            {
                "name": "simple",
                "config": {
                    "topics": [
                        {
                            "name": "t",
                            "partitions": [
                                {"partition": 0, "start_offset": 0, "end_offset": 10}
                            ],
                        }
                    ],
                    "cutoff_ms": 1000,
                    "batch_size": 5,
                },
                "batches": [
                    {
                        "messages": [
                            {
                                "topic": "t",
                                "partition": 0,
                                "offset": 0,
                                "timestamp_ms": 500,
                            }
                        ],
                        "expected_released": [
                            {"topic": "t", "partition": 0, "offset": 0}
                        ],
                    }
                ],
            }
        )
        assert result.passed
        assert len(result.batch_results) == 1
        assert result.batch_results[0].all_consumed

    def test_scenario_from_json_string(self):
        """Test running a scenario from a JSON string."""
        json_str = """
        {
            "name": "json string test",
            "config": {
                "topics": [{"name": "events", "partitions": [{"partition": 0, "start_offset": 0, "end_offset": 100}]}],
                "cutoff_ms": 2000,
                "batch_size": 10
            },
            "batches": [
                {
                    "messages": [
                        {"topic": "events", "partition": 0, "offset": 0, "timestamp_ms": 1000}
                    ],
                    "expected_released": [
                        {"topic": "events", "partition": 0, "offset": 0}
                    ]
                }
            ]
        }
        """
        result = run_scenario(json_str)
        assert result.passed


class TestScenarioFiles:
    """Test loading and running scenarios from files."""

    def test_basic_ordering_scenario(self):
        """Test the basic ordering scenario file."""
        result = run_scenario_file(SCENARIOS_DIR / "basic_ordering.json")
        result.assert_passed()

    def test_live_detection_scenario(self):
        """Test the live detection scenario file."""
        result = run_scenario_file(SCENARIOS_DIR / "live_detection.json")
        result.assert_passed()


class TestErrorHandling:
    """Test error handling in the testing framework."""

    def test_invalid_json_raises_value_error(self):
        """Test that invalid JSON raises a ValueError."""
        with pytest.raises(ValueError):
            run_scenario("not valid json")

    def test_missing_required_field_raises_value_error(self):
        """Test that missing required fields raise ValueError."""
        with pytest.raises(ValueError):
            run_scenario({"name": "incomplete"})

    def test_failed_scenario_details(self):
        """Test that failed scenarios provide useful error details."""
        # Intentionally wrong expectation
        result = run_scenario(
            {
                "name": "failing test",
                "config": {
                    "topics": [
                        {
                            "name": "t",
                            "partitions": [
                                {"partition": 0, "start_offset": 0, "end_offset": 10}
                            ],
                        }
                    ],
                    "cutoff_ms": 1000,
                    "batch_size": 5,
                },
                "batches": [
                    {
                        "messages": [
                            {
                                "topic": "t",
                                "partition": 0,
                                "offset": 0,
                                "timestamp_ms": 500,
                            }
                        ],
                        "expected_released": [
                            {"topic": "t", "partition": 0, "offset": 999}
                        ],  # Wrong offset
                    }
                ],
            }
        )
        assert not result.passed
        assert not result.batch_results[0].released_match
        assert len(result.batch_results[0].errors) > 0


class TestLowWaterMarkBehavior:
    """Test scenarios that verify low water mark behavior."""

    def test_messages_held_until_low_water_mark_advances(self):
        """Test that messages are held until all partitions catch up."""
        result = run_scenario(
            {
                "name": "low water mark test",
                "config": {
                    "topics": [
                        {
                            "name": "events",
                            "partitions": [
                                {"partition": 0, "start_offset": 0, "end_offset": 100},
                                {"partition": 1, "start_offset": 0, "end_offset": 100},
                            ],
                        }
                    ],
                    "cutoff_ms": 2000,
                    "batch_size": 10,
                },
                "batches": [
                    {
                        "description": "Partition 0 ahead, messages held",
                        "messages": [
                            {
                                "topic": "events",
                                "partition": 0,
                                "offset": 0,
                                "timestamp_ms": 1000,
                            },
                            {
                                "topic": "events",
                                "partition": 1,
                                "offset": 0,
                                "timestamp_ms": 500,
                            },
                        ],
                        "expected_released": [
                            {"topic": "events", "partition": 1, "offset": 0}
                        ],
                        "expected_partition_state": [
                            {"topic": "events", "partition": 0, "released_offset": 0},
                            {"topic": "events", "partition": 1, "released_offset": 0},
                        ],
                    },
                    {
                        "description": "Partition 1 catches up, partition 0 released",
                        "messages": [
                            {
                                "topic": "events",
                                "partition": 1,
                                "offset": 1,
                                "timestamp_ms": 1100,
                            }
                        ],
                        "expected_released": [
                            {"topic": "events", "partition": 0, "offset": 0}
                        ],
                        "expected_partition_state": [
                            {"topic": "events", "partition": 0, "released_offset": 0},
                            {"topic": "events", "partition": 1, "released_offset": 0},
                        ],
                    },
                ],
            }
        )
        result.assert_passed()
