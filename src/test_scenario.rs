//! Test scenario data structures for JSON-driven testing.

use serde::{Deserialize, Serialize};

/// A complete test scenario that can be loaded from JSON.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct TestScenario {
    /// Human-readable name for the test scenario.
    pub name: String,
    /// Configuration for the test.
    pub config: TestConfig,
    /// Batches of messages to process.
    pub batches: Vec<TestBatch>,
}

/// Configuration for a test scenario.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct TestConfig {
    /// Topics to consume from.
    pub topics: Vec<TopicConfig>,
    /// Cutoff timestamp in milliseconds.
    pub cutoff_ms: i64,
    /// Batch size for message release.
    pub batch_size: usize,
}

/// Configuration for a single topic.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct TopicConfig {
    /// Topic name.
    pub name: String,
    /// Partition configurations.
    pub partitions: Vec<PartitionConfig>,
}

/// Configuration for a single partition.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct PartitionConfig {
    /// Partition number.
    pub partition: i32,
    /// Start offset for replay.
    pub start_offset: i64,
    /// End offset (high watermark).
    pub end_offset: i64,
}

/// A batch of messages to be consumed.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct TestBatch {
    /// Optional description of what this batch tests.
    #[serde(default)]
    pub description: String,
    /// Messages that will come from Kafka in this batch.
    pub messages: Vec<TestMessage>,
    /// Expected messages to be released after processing this batch.
    #[serde(default)]
    pub expected_released: Vec<ExpectedMessage>,
    /// Expected partition state after processing this batch.
    #[serde(default)]
    pub expected_partition_state: Vec<ExpectedPartitionState>,
}

/// A message as it comes from Kafka.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct TestMessage {
    /// Topic name.
    pub topic: String,
    /// Partition number.
    pub partition: i32,
    /// Message offset.
    pub offset: i64,
    /// Message timestamp in milliseconds.
    pub timestamp_ms: i64,
    /// Optional message key (as string, will be converted to bytes).
    #[serde(default)]
    pub key: Option<String>,
    /// Optional message value (as string, will be converted to bytes).
    #[serde(default)]
    pub value: Option<String>,
}

/// Expected released message (only identifiers, since content is same as input).
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct ExpectedMessage {
    /// Topic name.
    pub topic: String,
    /// Partition number.
    pub partition: i32,
    /// Message offset.
    pub offset: i64,
}

/// Expected partition state after a batch.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ExpectedPartitionState {
    /// Topic name.
    pub topic: String,
    /// Partition number.
    pub partition: i32,
    /// Expected consumed offset (optional, not checked if None).
    #[serde(default)]
    pub consumed_offset: Option<i64>,
    /// Expected released offset (optional, not checked if None).
    #[serde(default)]
    pub released_offset: Option<i64>,
    /// Expected is_live state (optional, not checked if None).
    #[serde(default)]
    pub is_live: Option<bool>,
    /// Expected is_paused state (optional, not checked if None).
    #[serde(default)]
    pub is_paused: Option<bool>,
}

impl TestScenario {
    /// Load a test scenario from JSON string.
    pub fn from_json(json: &str) -> Result<Self, serde_json::Error> {
        serde_json::from_str(json)
    }

    /// Serialize the scenario to JSON string.
    pub fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string_pretty(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_minimal_scenario() {
        let json = r#"{
            "name": "minimal test",
            "config": {
                "topics": [{"name": "events", "partitions": [{"partition": 0, "start_offset": 0, "end_offset": 100}]}],
                "cutoff_ms": 2000,
                "batch_size": 10
            },
            "batches": []
        }"#;

        let scenario = TestScenario::from_json(json).unwrap();
        assert_eq!(scenario.name, "minimal test");
        assert_eq!(scenario.config.cutoff_ms, 2000);
        assert_eq!(scenario.config.batch_size, 10);
        assert_eq!(scenario.config.topics.len(), 1);
        assert_eq!(scenario.config.topics[0].name, "events");
        assert!(scenario.batches.is_empty());
    }

    #[test]
    fn test_parse_full_scenario() {
        let json = r#"{
            "name": "full test",
            "config": {
                "topics": [
                    {
                        "name": "events",
                        "partitions": [
                            {"partition": 0, "start_offset": 0, "end_offset": 100},
                            {"partition": 1, "start_offset": 0, "end_offset": 50}
                        ]
                    }
                ],
                "cutoff_ms": 2000,
                "batch_size": 10
            },
            "batches": [
                {
                    "description": "first batch",
                    "messages": [
                        {"topic": "events", "partition": 0, "offset": 0, "timestamp_ms": 1000, "key": "k1", "value": "v1"},
                        {"topic": "events", "partition": 1, "offset": 0, "timestamp_ms": 500}
                    ],
                    "expected_released": [
                        {"topic": "events", "partition": 1, "offset": 0}
                    ],
                    "expected_partition_state": [
                        {"topic": "events", "partition": 0, "consumed_offset": 0, "is_live": false},
                        {"topic": "events", "partition": 1, "consumed_offset": 0, "released_offset": 0, "is_live": false}
                    ]
                }
            ]
        }"#;

        let scenario = TestScenario::from_json(json).unwrap();
        assert_eq!(scenario.name, "full test");
        assert_eq!(scenario.batches.len(), 1);

        let batch = &scenario.batches[0];
        assert_eq!(batch.description, "first batch");
        assert_eq!(batch.messages.len(), 2);
        assert_eq!(batch.messages[0].key, Some("k1".to_string()));
        assert_eq!(batch.messages[0].value, Some("v1".to_string()));
        assert_eq!(batch.messages[1].key, None);

        assert_eq!(batch.expected_released.len(), 1);
        assert_eq!(batch.expected_released[0].partition, 1);

        assert_eq!(batch.expected_partition_state.len(), 2);
        assert_eq!(batch.expected_partition_state[0].consumed_offset, Some(0));
        assert_eq!(batch.expected_partition_state[0].released_offset, None);
    }

    #[test]
    fn test_roundtrip_serialization() {
        let scenario = TestScenario {
            name: "roundtrip test".to_string(),
            config: TestConfig {
                topics: vec![TopicConfig {
                    name: "test".to_string(),
                    partitions: vec![PartitionConfig {
                        partition: 0,
                        start_offset: 0,
                        end_offset: 100,
                    }],
                }],
                cutoff_ms: 1000,
                batch_size: 5,
            },
            batches: vec![],
        };

        let json = scenario.to_json().unwrap();
        let parsed = TestScenario::from_json(&json).unwrap();
        assert_eq!(parsed.name, scenario.name);
        assert_eq!(parsed.config.cutoff_ms, scenario.config.cutoff_ms);
    }
}
