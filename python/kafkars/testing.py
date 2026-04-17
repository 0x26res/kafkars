"""Testing utilities for kafkars.

This module provides a JSON-driven testing framework for validating
ConsumerManager behavior with mock Kafka consumers.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from kafkars._lib import run_test_scenario_with_data, run_test_scenario  # type: ignore[unresolved-import]


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


def _wrap_result(result: Any) -> TestResult:
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


def run_scenario(scenario: dict[str, Any] | str) -> TestResult:
    """Run a self-contained test scenario against the mock consumer.

    Args:
        scenario: Either a dict representing the scenario or a JSON string.

    Returns:
        TestResult with pass/fail status and detailed batch results.
    """
    scenario_json = json.dumps(scenario) if isinstance(scenario, dict) else scenario
    return _wrap_result(run_test_scenario(scenario_json))


def run_scenario_with_data(
    scenario: dict[str, Any] | str,
    data: dict[str, Any] | str,
) -> TestResult:
    """Run a split-format scenario (scenario + data) against the mock consumer.

    Args:
        scenario: Scenario spec as a dict or JSON string.
        data: Topic data as a dict or JSON string.

    Returns:
        TestResult with pass/fail status and detailed batch results.
    """
    scenario_json = json.dumps(scenario) if isinstance(scenario, dict) else scenario
    data_json = json.dumps(data) if isinstance(data, dict) else data
    return _wrap_result(run_test_scenario_with_data(scenario_json, data_json))


def load_scenario(path: str | Path) -> dict[str, Any]:
    """Load a test scenario from a JSON file."""
    with open(path) as f:
        return json.load(f)


def run_scenario_file(path: str | Path) -> TestResult:
    """Load and run a test scenario from a JSON file.

    Auto-detects format: if the scenario has a "data" key, resolves the data
    file from tests/data/ relative to the scenario's parent directory.
    """
    path = Path(path)
    scenario = load_scenario(path)

    if "data" in scenario:
        data_path = path.parent.parent / "data" / scenario["data"]
        with open(data_path) as f:
            data = json.load(f)
        return run_scenario_with_data(scenario, data)

    return run_scenario(scenario)
