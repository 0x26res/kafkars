//! Test runner for executing JSON-driven test scenarios.

use crate::consumer_manager::{ConsumerManager, PartitionStartOffset, StartOffsets};
use crate::mock_consumer::{MockConsumer, MockMessage};
use crate::test_scenario::{ExpectedMessage, TestScenario};
use std::time::Duration;

/// Result of running a complete test scenario.
#[derive(Debug, Clone)]
pub struct TestResult {
    /// Whether all batches passed.
    pub passed: bool,
    /// Results for each batch.
    pub batch_results: Vec<BatchResult>,
}

/// Result of running a single batch.
#[derive(Debug, Clone)]
pub struct BatchResult {
    /// Index of this batch in the scenario.
    pub batch_index: usize,
    /// Description of the batch (if provided).
    pub description: String,
    /// Whether this batch passed all checks.
    pub passed: bool,
    /// Detailed error messages for failures.
    pub errors: Vec<String>,
}

/// Run a test scenario and return the results.
pub fn run_scenario(scenario: &TestScenario) -> TestResult {
    // Build start offsets from config
    let mut start_offset_list = Vec::new();
    let mut topic_names = Vec::new();

    for topic in &scenario.config.topics {
        topic_names.push(topic.name.clone());
        for part in &topic.partitions {
            start_offset_list.push(PartitionStartOffset {
                topic: topic.name.clone(),
                partition: part.partition,
                replay_start_offset: part.start_offset,
                replay_end_offset: part.end_offset,
            });
        }
    }

    let start_offsets = StartOffsets::from_vec(start_offset_list);

    // Create mock consumer
    let mock = MockConsumer::new();

    // Create ConsumerManager with mock
    let mut manager = ConsumerManager::from_resolved(
        mock,
        start_offsets,
        topic_names,
        scenario.config.batch_size,
    );

    // Run each batch
    let mut batch_results = Vec::new();

    for (idx, batch) in scenario.batches.iter().enumerate() {
        let mut errors = Vec::new();

        // Add messages to mock
        let mock_messages: Vec<MockMessage> = batch
            .messages
            .iter()
            .map(|m| {
                let mut msg = MockMessage::new(&m.topic, m.partition, m.offset, m.timestamp_ms);
                if let Some(ref key) = m.key {
                    msg = msg.with_key(key.as_bytes());
                }
                if let Some(ref value) = m.value {
                    msg = msg.with_value(value.as_bytes());
                }
                msg
            })
            .collect();

        let expected_message_count = mock_messages.len();
        manager.consumer().add_message_batch(mock_messages);

        // Poll with timeout to trigger batch processing
        let released = manager
            .poll(Duration::from_millis(100))
            .expect("Poll should not fail in test");

        // Check if all batch messages were consumed
        let remaining = manager.consumer().remaining_message_count();
        let all_consumed = remaining == 0;
        if !all_consumed {
            errors.push(format!(
                "Not all batch messages were consumed: {} of {} remaining",
                remaining, expected_message_count
            ));
        }

        // Verify released messages
        let released_refs: Vec<ExpectedMessage> = released
            .iter()
            .map(|m| ExpectedMessage {
                topic: m.topic.clone(),
                partition: m.partition,
                offset: m.offset,
            })
            .collect();

        let released_match = released_refs == batch.expected_released;
        if !released_match {
            errors.push(format!(
                "Released messages mismatch:\n  expected: {:?}\n  actual: {:?}",
                batch.expected_released, released_refs
            ));
        }

        // Verify partition state
        let mut state_match = true;
        let partition_info = manager.partition_info();

        for expected in &batch.expected_partition_state {
            let key = (expected.topic.clone(), expected.partition);
            if let Some(actual) = partition_info.get(&key) {
                if let Some(exp_consumed) = expected.consumed_offset {
                    if actual.consumed_offset != exp_consumed {
                        state_match = false;
                        errors.push(format!(
                            "{}:{} consumed_offset: expected {}, got {}",
                            expected.topic,
                            expected.partition,
                            exp_consumed,
                            actual.consumed_offset
                        ));
                    }
                }
                if let Some(exp_released) = expected.released_offset {
                    if actual.released_offset != exp_released {
                        state_match = false;
                        errors.push(format!(
                            "{}:{} released_offset: expected {}, got {}",
                            expected.topic,
                            expected.partition,
                            exp_released,
                            actual.released_offset
                        ));
                    }
                }
                if let Some(exp_live) = expected.is_live {
                    if actual.is_live != exp_live {
                        state_match = false;
                        errors.push(format!(
                            "{}:{} is_live: expected {}, got {}",
                            expected.topic, expected.partition, exp_live, actual.is_live
                        ));
                    }
                }
                if let Some(exp_paused) = expected.is_paused {
                    if actual.is_paused != exp_paused {
                        state_match = false;
                        errors.push(format!(
                            "{}:{} is_paused: expected {}, got {}",
                            expected.topic, expected.partition, exp_paused, actual.is_paused
                        ));
                    }
                }
            } else {
                state_match = false;
                errors.push(format!(
                    "Partition {}:{} not found",
                    expected.topic, expected.partition
                ));
            }
        }

        let passed = released_match && state_match && all_consumed;

        batch_results.push(BatchResult {
            batch_index: idx,
            description: batch.description.clone(),
            passed,
            errors,
        });
    }

    let all_passed = batch_results.iter().all(|r| r.passed);

    TestResult {
        passed: all_passed,
        batch_results,
    }
}

/// Run a split-format scenario from files under `tests/`, panicking with
/// details on failure. `scenario` and `data` are file stems (without path or
/// extension) resolved to `tests/scenarios/{scenario}.json` and
/// `tests/data/{data}.json`.
pub fn assert_scenario(scenario: &str, data: &str) {
    let base = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests");
    let scenario_json =
        std::fs::read_to_string(base.join("scenarios").join(format!("{scenario}.json")))
            .unwrap_or_else(|e| panic!("failed to read scenario '{scenario}': {e}"));
    let data_json = std::fs::read_to_string(base.join("data").join(format!("{data}.json")))
        .unwrap_or_else(|e| panic!("failed to read data '{data}': {e}"));

    let spec = crate::test_scenario::ScenarioSpec::from_json(&scenario_json)
        .expect("invalid scenario JSON");
    let topic_data: crate::test_scenario::TopicData =
        serde_json::from_str(&data_json).expect("invalid data JSON");
    let resolved = spec
        .resolve(&topic_data)
        .expect("scenario resolution failed");
    let result = run_scenario(&resolved);
    assert!(
        result.passed,
        "Scenario '{}' failed:\n{}",
        resolved.name,
        result
            .batch_results
            .iter()
            .filter(|b| !b.passed)
            .map(|b| format!(
                "  batch {} ({}): {}",
                b.batch_index,
                b.description,
                b.errors.join(", ")
            ))
            .collect::<Vec<_>>()
            .join("\n")
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_scenario::TestScenario;

    // --- File-based scenario tests ---

    #[test]
    fn test_basic_ordering() {
        assert_scenario("basic_ordering", "two_partition_ordering");
    }

    #[test]
    fn test_live_detection() {
        assert_scenario("live_detection", "live_detection_events");
    }

    #[test]
    fn test_single_partition_live() {
        assert_scenario("single_partition_live", "single_partition");
    }

    #[test]
    fn test_single_partition_delayed_live() {
        assert_scenario("single_partition_delayed_live", "single_partition");
    }

    // --- Inline scenario tests ---

    #[test]
    fn test_run_simple_scenario() {
        let json = r#"{
            "name": "simple single message",
            "config": {
                "topics": [{"name": "events", "partitions": [{"partition": 0, "start_offset": 0, "end_offset": 100}]}],

                "batch_size": 10
            },
            "batches": [
                {
                    "description": "single message",
                    "messages": [
                        {"topic": "events", "partition": 0, "offset": 0, "timestamp_ms": 1000}
                    ],
                    "expected_released": [
                        {"topic": "events", "partition": 0, "offset": 0}
                    ],
                    "expected_partition_state": [
                        {"topic": "events", "partition": 0, "consumed_offset": 0, "released_offset": 0, "is_live": false}
                    ]
                }
            ]
        }"#;

        let scenario = TestScenario::from_json(json).unwrap();
        let result = run_scenario(&scenario);

        assert!(
            result.passed,
            "Scenario should pass: {:?}",
            result.batch_results
        );
        assert_eq!(result.batch_results.len(), 1);
        assert!(result.batch_results[0].passed);
    }

    #[test]
    fn test_two_partition_ordering() {
        // Test that messages are released based on low water mark.
        // With two partitions, the low water mark is the minimum timestamp
        // among non-live partitions. Messages are only released up to that mark.
        //
        // The key insight: low water mark = min(last_consumed_timestamp) across partitions.
        // Messages are released if timestamp <= low_water_mark.
        let json = r#"{
            "name": "two partition ordering",
            "config": {
                "topics": [{"name": "events", "partitions": [
                    {"partition": 0, "start_offset": 0, "end_offset": 100},
                    {"partition": 1, "start_offset": 0, "end_offset": 100}
                ]}],

                "batch_size": 10
            },
            "batches": [
                {
                    "description": "batch 1: interleaved timestamps - only up to low water mark released",
                    "messages": [
                        {"topic": "events", "partition": 0, "offset": 0, "timestamp_ms": 1000},
                        {"topic": "events", "partition": 1, "offset": 0, "timestamp_ms": 500},
                        {"topic": "events", "partition": 0, "offset": 1, "timestamp_ms": 1500},
                        {"topic": "events", "partition": 1, "offset": 1, "timestamp_ms": 800}
                    ],
                    "expected_released": [
                        {"topic": "events", "partition": 1, "offset": 0},
                        {"topic": "events", "partition": 1, "offset": 1}
                    ],
                    "expected_partition_state": [
                        {"topic": "events", "partition": 0, "consumed_offset": 1, "released_offset": 0, "is_live": false},
                        {"topic": "events", "partition": 1, "consumed_offset": 1, "released_offset": 1, "is_live": false}
                    ]
                },
                {
                    "description": "batch 2: partition 1 advances, low water mark moves up",
                    "messages": [
                        {"topic": "events", "partition": 1, "offset": 2, "timestamp_ms": 1600}
                    ],
                    "expected_released": [
                        {"topic": "events", "partition": 0, "offset": 0},
                        {"topic": "events", "partition": 0, "offset": 1}
                    ],
                    "expected_partition_state": [
                        {"topic": "events", "partition": 0, "consumed_offset": 1, "released_offset": 1, "is_live": false},
                        {"topic": "events", "partition": 1, "consumed_offset": 2, "released_offset": 1, "is_live": false}
                    ]
                },
                {
                    "description": "batch 3: partition 0 advances, low water mark = 1600",
                    "messages": [
                        {"topic": "events", "partition": 0, "offset": 2, "timestamp_ms": 1700}
                    ],
                    "expected_released": [
                        {"topic": "events", "partition": 1, "offset": 2}
                    ],
                    "expected_partition_state": [
                        {"topic": "events", "partition": 0, "consumed_offset": 2, "released_offset": 1, "is_live": false},
                        {"topic": "events", "partition": 1, "consumed_offset": 2, "released_offset": 2, "is_live": false}
                    ]
                },
                {
                    "description": "batch 4: partition 1 advances past partition 0, releases p0 messages",
                    "messages": [
                        {"topic": "events", "partition": 1, "offset": 3, "timestamp_ms": 1800}
                    ],
                    "expected_released": [
                        {"topic": "events", "partition": 0, "offset": 2}
                    ],
                    "expected_partition_state": [
                        {"topic": "events", "partition": 0, "consumed_offset": 2, "released_offset": 2, "is_live": false},
                        {"topic": "events", "partition": 1, "consumed_offset": 3, "released_offset": 2, "is_live": false}
                    ]
                }
            ]
        }"#;

        let scenario = TestScenario::from_json(json).unwrap();
        let result = run_scenario(&scenario);

        assert!(
            result.passed,
            "Scenario should pass: {:?}",
            result.batch_results
        );
    }

    #[test]
    fn test_end_offset_makes_live() {
        // Test that reaching end offset marks partition as live
        let json = r#"{
            "name": "end offset makes live",
            "config": {
                "topics": [{"name": "events", "partitions": [
                    {"partition": 0, "start_offset": 0, "end_offset": 1}
                ]}],

                "batch_size": 10
            },
            "batches": [
                {
                    "description": "last message before end offset",
                    "messages": [
                        {"topic": "events", "partition": 0, "offset": 0, "timestamp_ms": 500}
                    ],
                    "expected_released": [
                        {"topic": "events", "partition": 0, "offset": 0}
                    ],
                    "expected_partition_state": [
                        {"topic": "events", "partition": 0, "is_live": true}
                    ]
                }
            ]
        }"#;

        let scenario = TestScenario::from_json(json).unwrap();
        let result = run_scenario(&scenario);

        assert!(
            result.passed,
            "Scenario should pass: {:?}",
            result.batch_results
        );
    }

    #[test]
    fn test_empty_partition_is_live() {
        // A partition with start_offset == end_offset has nothing to consume,
        // so it should be live immediately.
        let json = r#"{
            "name": "empty partition is live",
            "config": {
                "topics": [{"name": "events", "partitions": [
                    {"partition": 0, "start_offset": 0, "end_offset": 0}
                ]}],

                "batch_size": 10
            },
            "batches": [
                {
                    "description": "no messages - partition is already live",
                    "messages": [],
                    "expected_partition_state": [
                        {"topic": "events", "partition": 0, "is_live": true}
                    ]
                }
            ]
        }"#;

        let scenario = TestScenario::from_json(json).unwrap();
        let result = run_scenario(&scenario);

        assert!(
            result.passed,
            "Scenario should pass: {:?}",
            result.batch_results
        );
    }
}
