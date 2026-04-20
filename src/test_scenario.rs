//! Test scenario data structures for JSON-driven testing.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

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

// --- Split-format types (data file + scenario file) ---

/// A message in a data file. Topic and partition come from the map keys.
#[derive(Debug, Clone, Deserialize)]
pub struct DataMessage {
    pub offset: i64,
    pub timestamp_ms: i64,
    #[serde(default)]
    pub key: Option<String>,
    #[serde(default)]
    pub value: Option<String>,
}

/// Parsed data file: topic name -> partition id (as string) -> messages.
pub type TopicData = HashMap<String, HashMap<String, Vec<DataMessage>>>;

/// Per-topic config in a scenario spec.
#[derive(Debug, Clone, Deserialize)]
pub struct TopicSpec {
    pub partitions: Vec<i32>,
    /// Override end offsets per partition. Absent -> derived from data (last offset + 1).
    #[serde(default)]
    pub end_offsets: HashMap<String, i64>,
}

/// Config section of a scenario spec.
#[derive(Debug, Clone, Deserialize)]
pub struct ScenarioConfig {
    pub topics: HashMap<String, TopicSpec>,
    pub batch_size: usize,
}

/// A batch in the scenario spec -- references offsets by id, not full messages.
#[derive(Debug, Clone, Deserialize)]
pub struct ScenarioBatch {
    #[serde(default)]
    pub description: String,
    /// topic -> partition (string) -> list of offsets to poll.
    pub poll: HashMap<String, HashMap<String, Vec<i64>>>,
    #[serde(default)]
    pub expected_released: Vec<ExpectedMessage>,
    #[serde(default)]
    pub expected_partition_state: Vec<ExpectedPartitionState>,
}

/// Scenario spec parsed from the new-format scenario file.
#[derive(Debug, Clone, Deserialize)]
pub struct ScenarioSpec {
    pub name: String,
    pub config: ScenarioConfig,
    pub batches: Vec<ScenarioBatch>,
}

impl ScenarioSpec {
    pub fn from_json(json: &str) -> Result<Self, serde_json::Error> {
        serde_json::from_str(json)
    }

    /// Resolve against topic data to produce a TestScenario the runner can execute.
    pub fn resolve(&self, data: &TopicData) -> Result<TestScenario, String> {
        let mut topic_names: Vec<&String> = self.config.topics.keys().collect();
        topic_names.sort();

        let mut topic_configs = Vec::new();
        for topic_name in &topic_names {
            let spec = &self.config.topics[*topic_name];
            let topic_data = data
                .get(*topic_name)
                .ok_or_else(|| format!("topic '{}' not found in data file", topic_name))?;

            let mut partitions_sorted = spec.partitions.clone();
            partitions_sorted.sort();

            let mut part_configs = Vec::new();
            for &pid in &partitions_sorted {
                let pid_str = pid.to_string();
                let msgs = topic_data.get(&pid_str).ok_or_else(|| {
                    format!(
                        "partition {} of topic '{}' not found in data",
                        pid, topic_name
                    )
                })?;
                if msgs.is_empty() {
                    return Err(format!("no messages for {}:{}", topic_name, pid));
                }
                let start_offset = msgs.iter().map(|m| m.offset).min().unwrap();
                let end_offset = spec
                    .end_offsets
                    .get(&pid_str)
                    .copied()
                    .unwrap_or_else(|| msgs.iter().map(|m| m.offset).max().unwrap() + 1);

                part_configs.push(PartitionConfig {
                    partition: pid,
                    start_offset,
                    end_offset,
                });
            }
            topic_configs.push(TopicConfig {
                name: (*topic_name).clone(),
                partitions: part_configs,
            });
        }

        let mut batches = Vec::new();
        for batch in &self.batches {
            let mut messages = Vec::new();
            for topic_name in &topic_names {
                if let Some(topic_poll) = batch.poll.get(*topic_name) {
                    let topic_data = &data[*topic_name];
                    let mut pids: Vec<&String> = topic_poll.keys().collect();
                    pids.sort();
                    for pid_str in pids {
                        let offsets = &topic_poll[pid_str];
                        let part_msgs = topic_data.get(pid_str).ok_or_else(|| {
                            format!(
                                "partition {} of topic '{}' not found in data",
                                pid_str, topic_name
                            )
                        })?;
                        for &off in offsets {
                            let dm =
                                part_msgs.iter().find(|m| m.offset == off).ok_or_else(|| {
                                    format!(
                                        "offset {} not found in {}:{}",
                                        off, topic_name, pid_str
                                    )
                                })?;
                            messages.push(TestMessage {
                                topic: (*topic_name).clone(),
                                partition: pid_str
                                    .parse()
                                    .map_err(|_| format!("bad partition id '{}'", pid_str))?,
                                offset: dm.offset,
                                timestamp_ms: dm.timestamp_ms,
                                key: dm.key.clone(),
                                value: dm.value.clone(),
                            });
                        }
                    }
                }
            }
            batches.push(TestBatch {
                description: batch.description.clone(),
                messages,
                expected_released: batch.expected_released.clone(),
                expected_partition_state: batch.expected_partition_state.clone(),
            });
        }

        Ok(TestScenario {
            name: self.name.clone(),
            config: TestConfig {
                topics: topic_configs,
                batch_size: self.config.batch_size,
            },
            batches,
        })
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

                "batch_size": 10
            },
            "batches": []
        }"#;

        let scenario = TestScenario::from_json(json).unwrap();
        assert_eq!(scenario.name, "minimal test");
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
                batch_size: 5,
            },
            batches: vec![],
        };

        let json = scenario.to_json().unwrap();
        let parsed = TestScenario::from_json(&json).unwrap();
        assert_eq!(parsed.name, scenario.name);
        assert_eq!(parsed.config.batch_size, scenario.config.batch_size);
    }

    // --- Split-format tests ---

    fn sample_data() -> TopicData {
        serde_json::from_str(
            r#"{
                "events": {
                    "0": [
                        {"offset": 0, "timestamp_ms": 500, "value": "p0-msg0"},
                        {"offset": 1, "timestamp_ms": 600}
                    ],
                    "1": [
                        {"offset": 0, "timestamp_ms": 400},
                        {"offset": 1, "timestamp_ms": 700}
                    ]
                }
            }"#,
        )
        .unwrap()
    }

    #[test]
    fn test_resolve_happy_path() {
        let spec = ScenarioSpec::from_json(
            r#"{
                "name": "resolve test",
                "config": {
                    "topics": {"events": {"partitions": [0, 1]}},
    
                    "batch_size": 10
                },
                "batches": [{
                    "description": "first poll",
                    "poll": {"events": {"0": [0], "1": [0]}}
                }]
            }"#,
        )
        .unwrap();

        let resolved = spec.resolve(&sample_data()).unwrap();
        assert_eq!(resolved.name, "resolve test");
        assert_eq!(resolved.config.topics.len(), 1);
        assert_eq!(resolved.config.topics[0].name, "events");

        let p0 = &resolved.config.topics[0].partitions[0];
        assert_eq!(p0.start_offset, 0);
        assert_eq!(p0.end_offset, 2); // derived: max(0,1) + 1

        let batch = &resolved.batches[0];
        assert_eq!(batch.messages.len(), 2);
        assert_eq!(batch.messages[0].topic, "events");
        assert_eq!(batch.messages[0].partition, 0);
        assert_eq!(batch.messages[0].offset, 0);
        assert_eq!(batch.messages[0].timestamp_ms, 500);
        assert_eq!(batch.messages[0].value, Some("p0-msg0".to_string()));
        assert_eq!(batch.messages[1].partition, 1);
        assert_eq!(batch.messages[1].timestamp_ms, 400);
    }

    #[test]
    fn test_resolve_explicit_end_offsets() {
        let spec = ScenarioSpec::from_json(
            r#"{
                "name": "end offset override",
                "config": {
                    "topics": {"events": {"partitions": [0], "end_offsets": {"0": 100}}},
    
                    "batch_size": 10
                },
                "batches": []
            }"#,
        )
        .unwrap();

        let resolved = spec.resolve(&sample_data()).unwrap();
        assert_eq!(resolved.config.topics[0].partitions[0].end_offset, 100);
    }

    #[test]
    fn test_resolve_missing_topic_error() {
        let spec = ScenarioSpec::from_json(
            r#"{
                "name": "bad topic",
                "config": {
                    "topics": {"missing": {"partitions": [0]}},
    
                    "batch_size": 10
                },
                "batches": []
            }"#,
        )
        .unwrap();

        let err = spec.resolve(&sample_data()).unwrap_err();
        assert!(err.contains("not found in data"), "got: {}", err);
    }

    #[test]
    fn test_resolve_missing_partition_error() {
        let spec = ScenarioSpec::from_json(
            r#"{
                "name": "bad partition",
                "config": {
                    "topics": {"events": {"partitions": [0, 99]}},
    
                    "batch_size": 10
                },
                "batches": []
            }"#,
        )
        .unwrap();

        let err = spec.resolve(&sample_data()).unwrap_err();
        assert!(err.contains("partition 99"), "got: {}", err);
    }

    #[test]
    fn test_resolve_missing_offset_error() {
        let spec = ScenarioSpec::from_json(
            r#"{
                "name": "bad offset",
                "config": {
                    "topics": {"events": {"partitions": [0]}},
    
                    "batch_size": 10
                },
                "batches": [{
                    "poll": {"events": {"0": [999]}}
                }]
            }"#,
        )
        .unwrap();

        let err = spec.resolve(&sample_data()).unwrap_err();
        assert!(err.contains("offset 999"), "got: {}", err);
    }
}
