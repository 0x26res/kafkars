use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::message::{BorrowedMessage, Message};
use rdkafka::topic_partition_list::Offset;
use rdkafka::TopicPartitionList;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use crate::source_topic::{OffsetPolicy, SourceTopic};

/// Immutable record of the resolved offsets for a partition at creation time.
#[derive(Debug, Clone, PartialEq)]
pub struct PartitionStartOffset {
    pub topic: String,
    pub partition: i32,
    /// The resolved start offset based on the topic's offset policy.
    pub replay_start_offset: i64,
    /// The high watermark (end offset) at the time of resolution.
    pub replay_end_offset: i64,
}

/// Immutable collection of start offsets resolved at consumer creation time.
#[derive(Debug, Clone)]
pub struct StartOffsets {
    offsets: Arc<Vec<PartitionStartOffset>>,
}

impl StartOffsets {
    fn new(offsets: Vec<PartitionStartOffset>) -> Self {
        Self {
            offsets: Arc::new(offsets),
        }
    }

    pub fn iter(&self) -> impl Iterator<Item = &PartitionStartOffset> {
        self.offsets.iter()
    }

    pub fn get(&self, topic: &str, partition: i32) -> Option<&PartitionStartOffset> {
        self.offsets
            .iter()
            .find(|o| o.topic == topic && o.partition == partition)
    }

    pub fn get_replay_start_offset(&self, topic: &str, partition: i32) -> Option<i64> {
        self.get(topic, partition).map(|o| o.replay_start_offset)
    }

    pub fn get_replay_end_offset(&self, topic: &str, partition: i32) -> Option<i64> {
        self.get(topic, partition).map(|o| o.replay_end_offset)
    }
}

#[derive(Debug, Clone)]
pub struct PartitionInfo {
    pub topic: String,
    pub partition: i32,
    /// Offset of last message consumed from Kafka.
    pub consumed_offset: i64,
    /// Offset of last message released via poll.
    pub released_offset: i64,
    /// Timestamp of last consumed message.
    pub timestamp_ms: Option<i64>,
    /// Whether this partition has caught up to replay_end_offset or cutoff.
    pub is_live: bool,
    /// Whether this partition is currently paused due to backpressure.
    pub is_paused: bool,
}

impl PartitionInfo {
    pub fn new(topic: String, partition: i32, start_offset: i64) -> Self {
        Self {
            topic,
            partition,
            consumed_offset: start_offset,
            released_offset: start_offset,
            timestamp_ms: None,
            is_live: false,
            is_paused: false,
        }
    }
}

#[derive(Debug, Clone)]
pub struct TimestampedMessage {
    pub key: Option<Vec<u8>>,
    pub value: Option<Vec<u8>>,
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub timestamp_ms: i64,
}

#[derive(Debug, Clone, Default)]
pub struct ConsumerMetrics {
    pub messages_consumed: u64,
    pub messages_released: u64,
    pub partitions_paused: u64,
    pub partitions_resumed: u64,
}

impl ConsumerMetrics {
    pub fn reset(&mut self) -> ConsumerMetrics {
        let snapshot = self.clone();
        *self = Self::default();
        snapshot
    }
}

/// Manages Kafka consumer with partition tracking, message buffering, and backpressure.
pub struct ConsumerManager {
    consumer: BaseConsumer,
    topic_names: Vec<String>,
    start_offsets: StartOffsets,
    cutoff_ms: i64,
    partition_info: HashMap<(String, i32), PartitionInfo>,
    held_messages: Vec<TimestampedMessage>,
    batch_size: usize,
    max_held_messages: usize,
    low_water_mark_ms: Option<i64>,
    paused_count: usize,
    metrics: ConsumerMetrics,
}

impl ConsumerManager {
    fn new(
        consumer: BaseConsumer,
        topic_names: Vec<String>,
        start_offsets: StartOffsets,
        cutoff_ms: i64,
        batch_size: usize,
    ) -> Self {
        // Initialize partition_info from start_offsets
        let partition_info = start_offsets
            .iter()
            .map(|so| {
                let key = (so.topic.clone(), so.partition);
                let info =
                    PartitionInfo::new(so.topic.clone(), so.partition, so.replay_start_offset);
                (key, info)
            })
            .collect();

        Self {
            consumer,
            topic_names,
            start_offsets,
            cutoff_ms,
            partition_info,
            held_messages: Vec::new(),
            batch_size,
            max_held_messages: batch_size * 5,
            low_water_mark_ms: None,
            paused_count: 0,
            metrics: ConsumerMetrics::default(),
        }
    }

    pub fn create(
        config: HashMap<String, String>,
        source_topics: Vec<SourceTopic>,
        cutoff_ms: i64,
        batch_size: usize,
    ) -> Result<Self, String> {
        // Validate that topic names are unique
        let mut seen_topics = HashSet::new();
        for topic in &source_topics {
            if !seen_topics.insert(&topic.name) {
                return Err(format!("duplicate topic: '{}'", topic.name));
            }
        }

        let mut client_config = ClientConfig::new();
        for (key, value) in &config {
            client_config.set(key, value);
        }
        let consumer: BaseConsumer = client_config.create().map_err(|e| e.to_string())?;

        let mut resolved_offsets: Vec<PartitionStartOffset> = Vec::new();
        let metadata_timeout = Duration::from_secs(10);
        let watermark_timeout = Duration::from_secs(5);

        // First pass: collect all topic/partition metadata
        let mut partitions_to_resolve: Vec<(String, i32, OffsetPolicy)> = Vec::new();

        for source_topic in &source_topics {
            let metadata = consumer
                .fetch_metadata(Some(&source_topic.name), metadata_timeout)
                .map_err(|e| {
                    format!(
                        "failed to fetch metadata for '{}': {}",
                        source_topic.name, e
                    )
                })?;

            let topic_metadata = metadata
                .topics()
                .iter()
                .find(|t| t.name() == source_topic.name)
                .ok_or_else(|| format!("topic '{}' not found", source_topic.name))?;

            if topic_metadata.partitions().is_empty() {
                return Err(format!("topic '{}' has no partitions", source_topic.name));
            }

            for partition in topic_metadata.partitions() {
                partitions_to_resolve.push((
                    source_topic.name.clone(),
                    partition.id(),
                    source_topic.offset_policy.clone(),
                ));
            }
        }

        // Second pass: resolve offsets for each partition
        for (topic, partition, policy) in partitions_to_resolve {
            let (replay_start_offset, replay_end_offset) =
                Self::resolve_offsets(&consumer, &topic, partition, &policy, watermark_timeout)?;
            resolved_offsets.push(PartitionStartOffset {
                topic,
                partition,
                replay_start_offset,
                replay_end_offset,
            });
        }

        // Build topic partition list from resolved offsets
        let mut tpl = TopicPartitionList::new();
        for pso in &resolved_offsets {
            tpl.add_partition_offset(
                &pso.topic,
                pso.partition,
                Offset::Offset(pso.replay_start_offset),
            )
            .map_err(|e| e.to_string())?;
        }

        consumer.assign(&tpl).map_err(|e| e.to_string())?;

        let topic_names = source_topics.into_iter().map(|t| t.name).collect();
        let start_offsets = StartOffsets::new(resolved_offsets);
        Ok(Self::new(
            consumer,
            topic_names,
            start_offsets,
            cutoff_ms,
            batch_size,
        ))
    }

    /// Resolves the start offset based on policy and fetches the end offset (high watermark).
    /// Returns (start_offset, end_offset).
    fn resolve_offsets(
        consumer: &BaseConsumer,
        topic: &str,
        partition: i32,
        policy: &OffsetPolicy,
        timeout: Duration,
    ) -> Result<(i64, i64), String> {
        // Always fetch watermarks to get the end offset
        let (low, high) = consumer
            .fetch_watermarks(topic, partition, timeout)
            .map_err(|e| {
                format!(
                    "failed to fetch watermarks for {}:{}: {}",
                    topic, partition, e
                )
            })?;

        let start_offset = match policy {
            OffsetPolicy::Latest => high,
            OffsetPolicy::Earliest => low,
            OffsetPolicy::Committed => {
                let mut tpl = TopicPartitionList::new();
                tpl.add_partition(topic, partition);
                let committed = consumer.committed_offsets(tpl, timeout).map_err(|e| {
                    format!(
                        "failed to fetch committed offset for {}:{}: {}",
                        topic, partition, e
                    )
                })?;

                if let Some(elem) = committed.elements().first() {
                    match elem.offset() {
                        Offset::Offset(o) => o,
                        _ => low, // No committed offset, fall back to earliest
                    }
                } else {
                    return Err(format!(
                        "failed to get committed offset for {}:{}",
                        topic, partition
                    ));
                }
            }
            OffsetPolicy::RelativeTime { ms } => {
                let now_ms = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map_err(|e| e.to_string())?
                    .as_millis() as i64;
                let target_ms = now_ms - ms;
                Self::offset_for_timestamp(consumer, topic, partition, target_ms, high, timeout)?
            }
            OffsetPolicy::AbsoluteTime { ms } => {
                Self::offset_for_timestamp(consumer, topic, partition, *ms, high, timeout)?
            }
            OffsetPolicy::StartOfDay { time_ms, .. } => {
                Self::offset_for_timestamp(consumer, topic, partition, *time_ms, high, timeout)?
            }
        };

        Ok((start_offset, high))
    }

    fn offset_for_timestamp(
        consumer: &BaseConsumer,
        topic: &str,
        partition: i32,
        timestamp_ms: i64,
        high_watermark: i64,
        timeout: Duration,
    ) -> Result<i64, String> {
        let mut tpl = TopicPartitionList::new();
        tpl.add_partition_offset(topic, partition, Offset::Offset(timestamp_ms))
            .map_err(|e| e.to_string())?;

        let result = consumer.offsets_for_times(tpl, timeout).map_err(|e| {
            format!(
                "failed to resolve offset for timestamp {} on {}:{}: {}",
                timestamp_ms, topic, partition, e
            )
        })?;

        if let Some(elem) = result.elements().first() {
            match elem.offset() {
                Offset::Offset(o) => Ok(o),
                _ => {
                    // Timestamp is beyond available data, use high watermark
                    Ok(high_watermark)
                }
            }
        } else {
            Err(format!(
                "failed to resolve offset for timestamp {} on {}:{}",
                timestamp_ms, topic, partition
            ))
        }
    }

    pub fn poll(&mut self, timeout: Duration) -> Result<Vec<TimestampedMessage>, String> {
        // First poll with the given timeout
        if !self.poll_one(timeout)? {
            // No message received, just do housekeeping and return
            self.update_low_water_mark();
            self.manage_paused_partitions();
            return Ok(self.release_messages());
        }

        // Message received, keep polling with zero timeout to batch messages
        while self.held_messages.len() < self.max_held_messages && self.poll_one(Duration::ZERO)? {}

        // Sort held messages by timestamp for ordered release
        self.held_messages.sort_by_key(|m| m.timestamp_ms);

        self.update_low_water_mark();
        self.manage_paused_partitions();
        Ok(self.release_messages())
    }

    /// Polls for a single message. Returns Ok(true) if a message was received,
    /// Ok(false) if no message, or Err if an error occurred.
    fn poll_one(&mut self, timeout: Duration) -> Result<bool, String> {
        let polled = self.consumer.poll(timeout);
        if let Some(result) = polled {
            match result {
                Ok(msg) => {
                    if let Some(timestamped) = Self::extract_message(&msg) {
                        self.update_partition_info(&timestamped);
                        self.held_messages.push(timestamped);
                        self.metrics.messages_consumed += 1;
                        return Ok(true);
                    }
                }
                Err(e) => {
                    return Err(format!(
                        "{} (subscribed topics: {})",
                        e,
                        self.topic_names.join(", ")
                    ));
                }
            }
        }
        Ok(false)
    }

    fn extract_message(msg: &BorrowedMessage) -> Option<TimestampedMessage> {
        let timestamp_ms = msg.timestamp().to_millis()?;

        Some(TimestampedMessage {
            key: msg.key().map(|k| k.to_vec()),
            value: msg.payload().map(|v| v.to_vec()),
            topic: msg.topic().to_string(),
            partition: msg.partition(),
            offset: msg.offset(),
            timestamp_ms,
        })
    }

    fn update_partition_info(&mut self, msg: &TimestampedMessage) {
        let key = (msg.topic.clone(), msg.partition);
        if let Some(info) = self.partition_info.get_mut(&key) {
            info.consumed_offset = msg.offset;
            info.timestamp_ms = Some(msg.timestamp_ms);
            // A partition is live if either:
            // 1. Message timestamp is >= cutoff time, OR
            // 2. We've reached the end offset captured at creation time
            let replay_end_offset = self
                .start_offsets
                .get_replay_end_offset(&msg.topic, msg.partition)
                .unwrap_or(i64::MAX);
            // offset is 0-indexed, replay_end_offset is the next offset to be written
            // so we're caught up when current_offset + 1 >= replay_end_offset
            info.is_live =
                msg.timestamp_ms >= self.cutoff_ms || (msg.offset + 1) >= replay_end_offset;
        }
    }

    fn update_low_water_mark(&mut self) {
        let non_live_timestamps: Vec<i64> = self
            .partition_info
            .values()
            .filter(|p| !p.is_live)
            .filter_map(|p| p.timestamp_ms)
            .collect();

        self.low_water_mark_ms = non_live_timestamps.into_iter().min();
    }

    fn get_limit(&self) -> i64 {
        match self.low_water_mark_ms {
            Some(lwm) => lwm.min(self.cutoff_ms),
            None => self.cutoff_ms,
        }
    }

    fn release_messages(&mut self) -> Vec<TimestampedMessage> {
        let limit = self.get_limit();

        // Find how many messages to release (up to batch_size, with timestamp <= limit)
        let release_count = self
            .held_messages
            .iter()
            .take(self.batch_size)
            .take_while(|m| m.timestamp_ms <= limit)
            .count();

        self.metrics.messages_released += release_count as u64;
        let released: Vec<TimestampedMessage> =
            self.held_messages.drain(0..release_count).collect();

        // Update released_offset for each partition
        for msg in &released {
            let key = (msg.topic.clone(), msg.partition);
            if let Some(info) = self.partition_info.get_mut(&key) {
                info.released_offset = msg.offset;
            }
        }

        released
    }

    fn manage_paused_partitions(&mut self) {
        if self.held_messages.len() > self.max_held_messages {
            self.pause_ahead_partitions();
        } else if self.paused_count > 0 && self.held_messages.len() < self.batch_size {
            self.resume_all_partitions();
        }
    }

    fn pause_ahead_partitions(&mut self) {
        let Some(lwm) = self.low_water_mark_ms else {
            return;
        };

        let mut to_pause = TopicPartitionList::new();
        let mut partitions_to_pause: Vec<(String, i32)> = Vec::new();

        for info in self.partition_info.values() {
            if let Some(ts) = info.timestamp_ms {
                if ts > lwm && !info.is_paused {
                    to_pause.add_partition(&info.topic, info.partition);
                    partitions_to_pause.push((info.topic.clone(), info.partition));
                }
            }
        }

        if to_pause.count() > 0 {
            if let Err(e) = self.consumer.pause(&to_pause) {
                eprintln!("Error pausing partitions: {}", e);
            } else {
                // Mark partitions as paused
                for (topic, partition) in partitions_to_pause {
                    if let Some(info) = self.partition_info.get_mut(&(topic, partition)) {
                        info.is_paused = true;
                    }
                }
                self.paused_count += to_pause.count();
                self.metrics.partitions_paused += to_pause.count() as u64;
            }
        }
    }

    fn resume_all_partitions(&mut self) {
        if let Ok(assignment) = self.consumer.assignment() {
            if let Err(e) = self.consumer.resume(&assignment) {
                eprintln!("Error resuming partitions: {}", e);
            } else {
                // Mark all partitions as not paused
                for info in self.partition_info.values_mut() {
                    info.is_paused = false;
                }
                self.metrics.partitions_resumed += self.paused_count as u64;
                self.paused_count = 0;
            }
        }
    }

    pub fn get_priming_watermark(&self) -> Option<i64> {
        if self.partition_info.values().any(|p| !p.is_live) {
            self.low_water_mark_ms
        } else {
            None
        }
    }

    pub fn is_live(&self) -> bool {
        !self.partition_info.is_empty() && self.partition_info.values().all(|p| p.is_live)
    }

    pub fn flush_metrics(&mut self) -> ConsumerMetrics {
        self.metrics.reset()
    }

    pub fn held_message_count(&self) -> usize {
        self.held_messages.len()
    }

    pub fn paused_partition_count(&self) -> usize {
        self.paused_count
    }

    pub fn partition_info(&self) -> &HashMap<(String, i32), PartitionInfo> {
        &self.partition_info
    }

    pub fn start_offsets(&self) -> &StartOffsets {
        &self.start_offsets
    }

    pub fn cutoff_ms(&self) -> i64 {
        self.cutoff_ms
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_partition_info_new() {
        let info = PartitionInfo::new("test-topic".to_string(), 0, 100);
        assert_eq!(info.topic, "test-topic");
        assert_eq!(info.partition, 0);
        assert_eq!(info.consumed_offset, 100);
        assert_eq!(info.released_offset, 100);
        assert!(!info.is_paused);
        assert!(info.timestamp_ms.is_none());
        assert!(!info.is_live);
    }

    #[test]
    fn test_start_offsets() {
        let offsets = StartOffsets::new(vec![
            PartitionStartOffset {
                topic: "topic_1".to_string(),
                partition: 0,
                replay_start_offset: 100,
                replay_end_offset: 500,
            },
            PartitionStartOffset {
                topic: "topic_1".to_string(),
                partition: 1,
                replay_start_offset: 200,
                replay_end_offset: 600,
            },
        ]);

        assert_eq!(offsets.get_replay_start_offset("topic_1", 0), Some(100));
        assert_eq!(offsets.get_replay_start_offset("topic_1", 1), Some(200));
        assert_eq!(offsets.get_replay_end_offset("topic_1", 0), Some(500));
        assert_eq!(offsets.get_replay_end_offset("topic_1", 1), Some(600));
        assert_eq!(offsets.get("topic_1", 2), None);
        assert_eq!(offsets.get("topic_2", 0), None);
    }

    #[test]
    fn test_consumer_metrics_reset() {
        let mut metrics = ConsumerMetrics {
            messages_consumed: 100,
            messages_released: 90,
            partitions_paused: 5,
            partitions_resumed: 3,
        };

        let snapshot = metrics.reset();

        assert_eq!(snapshot.messages_consumed, 100);
        assert_eq!(snapshot.messages_released, 90);
        assert_eq!(metrics.messages_consumed, 0);
        assert_eq!(metrics.messages_released, 0);
    }

    #[test]
    fn test_timestamped_message() {
        let msg = TimestampedMessage {
            key: Some(b"key1".to_vec()),
            value: Some(b"value1".to_vec()),
            topic: "test".to_string(),
            partition: 0,
            offset: 42,
            timestamp_ms: 1_000_000,
        };

        assert_eq!(msg.key, Some(b"key1".to_vec()));
        assert_eq!(msg.offset, 42);
        assert_eq!(msg.timestamp_ms, 1_000_000);
    }

    #[test]
    fn test_duplicate_topics_rejected() {
        let config = HashMap::new();
        let topics = vec![
            SourceTopic::from_latest("topic_1".to_string()),
            SourceTopic::from_earliest("topic_1".to_string()),
        ];

        let result = ConsumerManager::create(config, topics, 0, 100);
        match result {
            Err(e) => assert!(
                e.contains("duplicate topic"),
                "expected duplicate topic error, got: {}",
                e
            ),
            Ok(_) => panic!("expected error for duplicate topics"),
        }
    }
}
