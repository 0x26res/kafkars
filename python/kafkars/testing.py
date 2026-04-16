"""Testing utilities for kafkars.

This module provides a JSON-driven testing framework for validating
ConsumerManager behavior with mock Kafka consumers.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from kafkars._lib import run_test_scenario as _run_test_scenario


@dataclass
class BatchResult:
    """Result of running a single batch in a test scenario."""

    batch_index: int
    description: str
    passed: bool
    released_match: bool
    state_match: bool
    all_consumed: bool
    errors: list[str]


@dataclass
class TestResult:
    """Result of running a complete test scenario."""

    passed: bool
    batch_results: list[BatchResult]

    def assert_passed(self) -> None:
        """Assert that all batches passed, raising AssertionError with details if not."""
        if not self.passed:
            failures = [
                f"Batch {br.batch_index} ({br.description}): {', '.join(br.errors)}"
                for br in self.batch_results
                if not br.passed
            ]
            raise AssertionError("Test scenario failed:\n" + "\n".join(failures))


def run_scenario(scenario: dict[str, Any] | str) -> TestResult:
    """Run a test scenario against the mock consumer.

    Args:
        scenario: Either a dict representing the scenario or a JSON string.

    Returns:
        TestResult with pass/fail status and detailed batch results.

    Example:
        >>> result = run_scenario({
        ...     "name": "simple",
        ...     "config": {
        ...         "topics": [{"name": "t", "partitions": [{"partition": 0, "start_offset": 0, "end_offset": 10}]}],
        ...         "cutoff_ms": 1000,
        ...         "batch_size": 5
        ...     },
        ...     "batches": [{
        ...         "messages": [{"topic": "t", "partition": 0, "offset": 0, "timestamp_ms": 500}],
        ...         "expected_released": [{"topic": "t", "partition": 0, "offset": 0}]
        ...     }]
        ... })
        >>> assert result.passed
    """
    if isinstance(scenario, dict):
        scenario_json = json.dumps(scenario)
    else:
        scenario_json = scenario

    result = _run_test_scenario(scenario_json)

    return TestResult(
        passed=result.passed,
        batch_results=[
            BatchResult(
                batch_index=br.batch_index,
                description=br.description,
                passed=br.passed,
                released_match=br.released_match,
                state_match=br.state_match,
                all_consumed=br.all_consumed,
                errors=list(br.errors),
            )
            for br in result.batch_results
        ],
    )


def load_scenario(path: str | Path) -> dict[str, Any]:
    """Load a test scenario from a JSON file.

    Args:
        path: Path to the JSON scenario file.

    Returns:
        Parsed scenario as a dictionary.
    """
    with open(path) as f:
        return json.load(f)


def run_scenario_file(path: str | Path) -> TestResult:
    """Load and run a test scenario from a JSON file.

    Args:
        path: Path to the JSON scenario file.

    Returns:
        TestResult with pass/fail status and detailed batch results.
    """
    scenario = load_scenario(path)
    return run_scenario(scenario)
