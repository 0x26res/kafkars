//! Mock Kafka consumer for testing ConsumerManager.

use crate::consumer_trait::{KafkaConsumer, MessageExtractor};
use rdkafka::error::KafkaResult;
use rdkafka::topic_partition_list::Offset;
use rdkafka::TopicPartitionList;
use std::cell::RefCell;
use std::collections::{HashSet, VecDeque};
use std::time::Duration;

/// A mock message for testing.
#[derive(Debug, Clone)]
pub struct MockMessage {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub key: Option<Vec<u8>>,
    pub value: Option<Vec<u8>>,
    pub timestamp_ms: i64,
}

impl MockMessage {
    pub fn new(topic: &str, partition: i32, offset: i64, timestamp_ms: i64) -> Self {
        Self {
            topic: topic.to_string(),
            partition,
            offset,
            key: None,
            value: None,
            timestamp_ms,
        }
    }

    pub fn with_key(mut self, key: &[u8]) -> Self {
        self.key = Some(key.to_vec());
        self
    }

    pub fn with_value(mut self, value: &[u8]) -> Self {
        self.value = Some(value.to_vec());
        self
    }
}

impl MessageExtractor for MockMessage {
    fn topic(&self) -> &str {
        &self.topic
    }

    fn partition(&self) -> i32 {
        self.partition
    }

    fn offset(&self) -> i64 {
        self.offset
    }

    fn timestamp_ms(&self) -> Option<i64> {
        Some(self.timestamp_ms)
    }

    fn key(&self) -> Option<&[u8]> {
        self.key.as_deref()
    }

    fn payload(&self) -> Option<&[u8]> {
        self.value.as_deref()
    }
}

/// Mock consumer state tracked internally.
#[derive(Debug, Default)]
struct MockConsumerState {
    /// Messages queued to be returned by poll, organized by batch.
    /// Each batch represents messages returned by a single poll with non-zero timeout.
    message_batches: VecDeque<VecDeque<MockMessage>>,
    /// Current batch being consumed.
    current_batch: VecDeque<MockMessage>,
    /// Assigned partitions.
    assigned: TopicPartitionList,
    /// Paused partitions.
    paused: HashSet<(String, i32)>,
    /// Track all poll calls for verification.
    poll_count: usize,
}

/// A mock Kafka consumer for testing ConsumerManager.
///
/// This mock simulates Kafka consumer behavior for testing purposes.
/// Messages are organized into batches, where each batch represents
/// messages returned by a single poll call with non-zero timeout.
pub struct MockConsumer {
    state: RefCell<MockConsumerState>,
}

impl Default for MockConsumer {
    fn default() -> Self {
        Self::new()
    }
}

impl MockConsumer {
    pub fn new() -> Self {
        Self {
            state: RefCell::new(MockConsumerState::default()),
        }
    }

    /// Add a batch of messages to be returned by poll.
    /// Messages within a batch are returned one at a time by successive poll calls.
    /// When a batch is exhausted, the next poll with non-zero timeout moves to next batch.
    pub fn add_message_batch(&self, messages: Vec<MockMessage>) {
        self.state
            .borrow_mut()
            .message_batches
            .push_back(messages.into());
    }

    /// Check if all batches have been fully consumed.
    pub fn all_messages_consumed(&self) -> bool {
        let state = self.state.borrow();
        state.current_batch.is_empty() && state.message_batches.is_empty()
    }

    /// Get the total number of poll calls made.
    pub fn poll_count(&self) -> usize {
        self.state.borrow().poll_count
    }

    /// Get remaining message count across all batches.
    pub fn remaining_message_count(&self) -> usize {
        let state = self.state.borrow();
        let current = state.current_batch.len();
        let batched: usize = state.message_batches.iter().map(|b| b.len()).sum();
        current + batched
    }

    /// Check if a partition is currently paused.
    pub fn is_paused(&self, topic: &str, partition: i32) -> bool {
        self.state
            .borrow()
            .paused
            .contains(&(topic.to_string(), partition))
    }

    /// Get the count of paused partitions.
    pub fn paused_count(&self) -> usize {
        self.state.borrow().paused.len()
    }
}

impl KafkaConsumer for MockConsumer {
    type Message<'a> = MockMessage;

    fn poll(&self, timeout: Duration) -> Option<KafkaResult<Self::Message<'_>>> {
        let mut state = self.state.borrow_mut();
        state.poll_count += 1;

        // If current batch is empty and we have a non-zero timeout, load next batch
        if state.current_batch.is_empty() {
            if timeout.is_zero() {
                // Zero timeout: return None (simulates no more messages available)
                return None;
            }
            // Non-zero timeout: load next batch
            if let Some(next_batch) = state.message_batches.pop_front() {
                state.current_batch = next_batch;
            } else {
                return None;
            }
        }

        // Return next message from current batch (if partition not paused)
        while let Some(msg) = state.current_batch.pop_front() {
            let key = (msg.topic.clone(), msg.partition);
            if !state.paused.contains(&key) {
                return Some(Ok(msg));
            }
            // Skip paused partition messages - they would be buffered in real Kafka
            // For simplicity in testing, we just skip them
        }

        None
    }

    fn assign(&self, assignment: &TopicPartitionList) -> KafkaResult<()> {
        self.state.borrow_mut().assigned = assignment.clone();
        Ok(())
    }

    fn seek(
        &self,
        _topic: &str,
        _partition: i32,
        _offset: Offset,
        _timeout: Duration,
    ) -> KafkaResult<()> {
        // Mock: just acknowledge the seek
        Ok(())
    }

    fn pause(&self, partitions: &TopicPartitionList) -> KafkaResult<()> {
        let mut state = self.state.borrow_mut();
        for elem in partitions.elements() {
            state
                .paused
                .insert((elem.topic().to_string(), elem.partition()));
        }
        Ok(())
    }

    fn resume(&self, partitions: &TopicPartitionList) -> KafkaResult<()> {
        let mut state = self.state.borrow_mut();
        for elem in partitions.elements() {
            state
                .paused
                .remove(&(elem.topic().to_string(), elem.partition()));
        }
        Ok(())
    }

    fn assignment(&self) -> KafkaResult<TopicPartitionList> {
        Ok(self.state.borrow().assigned.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mock_message_basic() {
        let msg = MockMessage::new("test-topic", 0, 42, 1000);
        assert_eq!(msg.topic(), "test-topic");
        assert_eq!(msg.partition(), 0);
        assert_eq!(msg.offset(), 42);
        assert_eq!(msg.timestamp_ms(), Some(1000));
        assert!(msg.key().is_none());
        assert!(msg.payload().is_none());
    }

    #[test]
    fn test_mock_message_with_data() {
        let msg = MockMessage::new("test-topic", 0, 42, 1000)
            .with_key(b"key1")
            .with_value(b"value1");

        assert_eq!(msg.key(), Some(b"key1".as_slice()));
        assert_eq!(msg.payload(), Some(b"value1".as_slice()));
    }

    #[test]
    fn test_mock_consumer_poll_empty() {
        let consumer = MockConsumer::new();
        let result = consumer.poll(Duration::from_millis(100));
        assert!(result.is_none());
    }

    #[test]
    fn test_mock_consumer_poll_with_messages() {
        let consumer = MockConsumer::new();
        consumer.add_message_batch(vec![
            MockMessage::new("topic", 0, 0, 1000),
            MockMessage::new("topic", 0, 1, 1001),
        ]);

        // First poll with timeout loads the batch and returns first message
        let msg1 = consumer.poll(Duration::from_millis(100)).unwrap().unwrap();
        assert_eq!(msg1.offset, 0);

        // Second poll with zero timeout returns from same batch
        let msg2 = consumer.poll(Duration::ZERO).unwrap().unwrap();
        assert_eq!(msg2.offset, 1);

        // Third poll with zero timeout returns None (batch exhausted)
        assert!(consumer.poll(Duration::ZERO).is_none());
    }

    #[test]
    fn test_mock_consumer_multiple_batches() {
        let consumer = MockConsumer::new();
        consumer.add_message_batch(vec![MockMessage::new("topic", 0, 0, 1000)]);
        consumer.add_message_batch(vec![MockMessage::new("topic", 0, 1, 2000)]);

        // First batch
        let msg1 = consumer.poll(Duration::from_millis(100)).unwrap().unwrap();
        assert_eq!(msg1.offset, 0);

        // Zero timeout returns None (batch exhausted)
        assert!(consumer.poll(Duration::ZERO).is_none());

        // Non-zero timeout loads next batch
        let msg2 = consumer.poll(Duration::from_millis(100)).unwrap().unwrap();
        assert_eq!(msg2.offset, 1);
    }

    #[test]
    fn test_mock_consumer_pause_resume() {
        let consumer = MockConsumer::new();

        let mut tpl = TopicPartitionList::new();
        tpl.add_partition("topic", 0);

        assert!(!consumer.is_paused("topic", 0));

        consumer.pause(&tpl).unwrap();
        assert!(consumer.is_paused("topic", 0));

        consumer.resume(&tpl).unwrap();
        assert!(!consumer.is_paused("topic", 0));
    }

    #[test]
    fn test_mock_consumer_paused_partition_skipped() {
        let consumer = MockConsumer::new();
        consumer.add_message_batch(vec![
            MockMessage::new("topic", 0, 0, 1000),
            MockMessage::new("topic", 1, 0, 1001),
        ]);

        // Pause partition 0
        let mut tpl = TopicPartitionList::new();
        tpl.add_partition("topic", 0);
        consumer.pause(&tpl).unwrap();

        // Poll should skip partition 0 and return partition 1
        let msg = consumer.poll(Duration::from_millis(100)).unwrap().unwrap();
        assert_eq!(msg.partition, 1);
    }

    #[test]
    fn test_mock_consumer_remaining_count() {
        let consumer = MockConsumer::new();
        assert_eq!(consumer.remaining_message_count(), 0);

        consumer.add_message_batch(vec![
            MockMessage::new("topic", 0, 0, 1000),
            MockMessage::new("topic", 0, 1, 1001),
        ]);
        assert_eq!(consumer.remaining_message_count(), 2);

        consumer.poll(Duration::from_millis(100));
        assert_eq!(consumer.remaining_message_count(), 1);
    }

    #[test]
    fn test_mock_consumer_all_consumed() {
        let consumer = MockConsumer::new();
        assert!(consumer.all_messages_consumed());

        consumer.add_message_batch(vec![MockMessage::new("topic", 0, 0, 1000)]);
        assert!(!consumer.all_messages_consumed());

        consumer.poll(Duration::from_millis(100));
        assert!(consumer.all_messages_consumed());
    }
}
