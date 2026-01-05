use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::message::{BorrowedMessage, Message};
use rdkafka::TopicPartitionList;
use std::collections::{HashMap, VecDeque};
use std::time::Duration;

/// Information about a partition's consumption state
#[derive(Debug, Clone)]
pub struct PartitionInfo {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub timestamp_ms: Option<i64>,
    pub is_live: bool,
}

impl PartitionInfo {
    pub fn new(topic: String, partition: i32) -> Self {
        Self {
            topic,
            partition,
            offset: 0,
            timestamp_ms: None,
            is_live: false,
        }
    }
}

/// A message with its timestamp for ordering
#[derive(Debug, Clone)]
pub struct TimestampedMessage {
    pub key: Option<Vec<u8>>,
    pub value: Option<Vec<u8>>,
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub timestamp_ms: i64,
}

/// Metrics for consumption performance
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

/// Manages Kafka consumer with partition tracking and message buffering.
///
/// Implements backpressure by pausing partitions that are ahead of the
/// low water mark when the held message buffer exceeds capacity.
pub struct ConsumerManager {
    consumer: BaseConsumer,
    cutoff_ms: i64,
    partition_info: HashMap<(String, i32), PartitionInfo>,
    held_messages: VecDeque<TimestampedMessage>,
    batch_size: usize,
    max_held_messages: usize,
    low_water_mark_ms: Option<i64>,
    paused_count: usize,
    metrics: ConsumerMetrics,
}

impl ConsumerManager {
    /// Create a new ConsumerManager
    pub fn new(consumer: BaseConsumer, cutoff_ms: i64, batch_size: usize) -> Self {
        Self {
            consumer,
            cutoff_ms,
            partition_info: HashMap::new(),
            held_messages: VecDeque::new(),
            batch_size,
            max_held_messages: batch_size * 5,
            low_water_mark_ms: None,
            paused_count: 0,
            metrics: ConsumerMetrics::default(),
        }
    }

    /// Poll for messages and return those ready to be released
    pub fn poll(&mut self, timeout: Duration) -> Vec<TimestampedMessage> {
        // Poll for new messages - extract data immediately to avoid borrow issues
        let polled = self.consumer.poll(timeout);
        if let Some(result) = polled {
            match result {
                Ok(msg) => {
                    if let Some(timestamped) = Self::extract_message(&msg) {
                        self.update_partition_info(&timestamped);
                        self.held_messages.push_back(timestamped);
                        self.metrics.messages_consumed += 1;
                    }
                }
                Err(e) => {
                    eprintln!("Error polling: {}", e);
                }
            }
        }

        // Update low water mark
        self.update_low_water_mark();

        // Manage paused partitions (backpressure)
        self.manage_paused_partitions();

        // Release messages up to the limit
        self.release_messages()
    }

    /// Extract data from a raw Kafka message into a TimestampedMessage
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

    /// Update partition info based on a consumed message
    fn update_partition_info(&mut self, msg: &TimestampedMessage) {
        let key = (msg.topic.clone(), msg.partition);
        let info = self
            .partition_info
            .entry(key)
            .or_insert_with(|| PartitionInfo::new(msg.topic.clone(), msg.partition));

        info.offset = msg.offset;
        info.timestamp_ms = Some(msg.timestamp_ms);
        info.is_live = msg.timestamp_ms >= self.cutoff_ms;
    }

    /// Update the low water mark based on non-live partitions
    fn update_low_water_mark(&mut self) {
        let non_live_timestamps: Vec<i64> = self
            .partition_info
            .values()
            .filter(|p| !p.is_live)
            .filter_map(|p| p.timestamp_ms)
            .collect();

        self.low_water_mark_ms = non_live_timestamps.into_iter().min();
    }

    /// Get the limit timestamp for releasing messages
    fn get_limit(&self) -> i64 {
        match self.low_water_mark_ms {
            Some(lwm) => lwm.min(self.cutoff_ms),
            None => self.cutoff_ms,
        }
    }

    /// Release messages that are below the current limit
    fn release_messages(&mut self) -> Vec<TimestampedMessage> {
        let limit = self.get_limit();
        let mut released = Vec::new();

        while let Some(msg) = self.held_messages.front() {
            if msg.timestamp_ms <= limit {
                if let Some(msg) = self.held_messages.pop_front() {
                    released.push(msg);
                    self.metrics.messages_released += 1;
                }
            } else {
                break;
            }
        }

        released
    }

    /// Manage partition pausing/resuming based on buffer pressure
    fn manage_paused_partitions(&mut self) {
        if self.held_messages.len() > self.max_held_messages {
            // Pause partitions that are ahead of the low water mark
            self.pause_ahead_partitions();
        } else if self.paused_count > 0 && self.held_messages.len() < self.batch_size {
            // Resume all partitions when buffer is low
            self.resume_all_partitions();
        }
    }

    /// Pause partitions whose timestamp is ahead of the low water mark
    fn pause_ahead_partitions(&mut self) {
        let Some(lwm) = self.low_water_mark_ms else {
            return;
        };

        let mut to_pause = TopicPartitionList::new();

        for info in self.partition_info.values() {
            if let Some(ts) = info.timestamp_ms {
                if ts > lwm {
                    to_pause.add_partition(&info.topic, info.partition);
                }
            }
        }

        if to_pause.count() > 0 {
            if let Err(e) = self.consumer.pause(&to_pause) {
                eprintln!("Error pausing partitions: {}", e);
            } else {
                self.paused_count += to_pause.count();
                self.metrics.partitions_paused += to_pause.count() as u64;
            }
        }
    }

    /// Resume all paused partitions
    fn resume_all_partitions(&mut self) {
        if let Ok(assignment) = self.consumer.assignment() {
            if let Err(e) = self.consumer.resume(&assignment) {
                eprintln!("Error resuming partitions: {}", e);
            } else {
                self.metrics.partitions_resumed += self.paused_count as u64;
                self.paused_count = 0;
            }
        }
    }

    /// Get the current low water mark if still priming (replaying historical data)
    pub fn get_priming_watermark(&self) -> Option<i64> {
        if self.partition_info.values().any(|p| !p.is_live) {
            self.low_water_mark_ms
        } else {
            None
        }
    }

    /// Check if all partitions have caught up to the cutoff
    pub fn is_live(&self) -> bool {
        !self.partition_info.is_empty() && self.partition_info.values().all(|p| p.is_live)
    }

    /// Flush and return current metrics, resetting counters
    pub fn flush_metrics(&mut self) -> ConsumerMetrics {
        self.metrics.reset()
    }

    /// Get the number of held messages
    pub fn held_message_count(&self) -> usize {
        self.held_messages.len()
    }

    /// Get the number of paused partitions
    pub fn paused_partition_count(&self) -> usize {
        self.paused_count
    }

    /// Get partition info for monitoring
    pub fn partition_info(&self) -> &HashMap<(String, i32), PartitionInfo> {
        &self.partition_info
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_partition_info_new() {
        let info = PartitionInfo::new("test-topic".to_string(), 0);
        assert_eq!(info.topic, "test-topic");
        assert_eq!(info.partition, 0);
        assert_eq!(info.offset, 0);
        assert!(info.timestamp_ms.is_none());
        assert!(!info.is_live);
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
}
