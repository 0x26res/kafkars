//! Traits for abstracting Kafka consumer operations to enable mocking.

use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::error::KafkaResult;
use rdkafka::message::{BorrowedMessage, Message};
use rdkafka::topic_partition_list::Offset;
use rdkafka::TopicPartitionList;
use std::time::Duration;

/// Abstraction over Kafka consumer operations to enable mocking.
///
/// This trait captures all the operations that ConsumerManager needs
/// from rdkafka's BaseConsumer, allowing for test doubles.
pub trait KafkaConsumer {
    /// The message type returned by poll operations.
    type Message<'a>
    where
        Self: 'a;

    /// Poll for the next message, returning None if timeout elapsed.
    fn poll(&self, timeout: Duration) -> Option<KafkaResult<Self::Message<'_>>>;

    /// Assign partitions to this consumer.
    fn assign(&self, assignment: &TopicPartitionList) -> KafkaResult<()>;

    /// Seek to a specific offset on a partition.
    fn seek(
        &self,
        topic: &str,
        partition: i32,
        offset: Offset,
        timeout: Duration,
    ) -> KafkaResult<()>;

    /// Pause consumption from the specified partitions.
    fn pause(&self, partitions: &TopicPartitionList) -> KafkaResult<()>;

    /// Resume consumption from the specified partitions.
    fn resume(&self, partitions: &TopicPartitionList) -> KafkaResult<()>;

    /// Get the current partition assignment.
    fn assignment(&self) -> KafkaResult<TopicPartitionList>;
}

/// Trait for extracting message data uniformly across real and mock messages.
pub trait MessageExtractor {
    fn topic(&self) -> &str;
    fn partition(&self) -> i32;
    fn offset(&self) -> i64;
    fn timestamp_ms(&self) -> Option<i64>;
    fn key(&self) -> Option<&[u8]>;
    fn payload(&self) -> Option<&[u8]>;
}

// Implementation for rdkafka's BaseConsumer
impl KafkaConsumer for BaseConsumer {
    type Message<'a> = BorrowedMessage<'a>;

    fn poll(&self, timeout: Duration) -> Option<KafkaResult<Self::Message<'_>>> {
        // poll is a direct method on BaseConsumer, not on Consumer trait
        BaseConsumer::poll(self, timeout)
    }

    fn assign(&self, assignment: &TopicPartitionList) -> KafkaResult<()> {
        <Self as Consumer>::assign(self, assignment)
    }

    fn seek(
        &self,
        topic: &str,
        partition: i32,
        offset: Offset,
        timeout: Duration,
    ) -> KafkaResult<()> {
        <Self as Consumer>::seek(self, topic, partition, offset, timeout)
    }

    fn pause(&self, partitions: &TopicPartitionList) -> KafkaResult<()> {
        <Self as Consumer>::pause(self, partitions)
    }

    fn resume(&self, partitions: &TopicPartitionList) -> KafkaResult<()> {
        <Self as Consumer>::resume(self, partitions)
    }

    fn assignment(&self) -> KafkaResult<TopicPartitionList> {
        <Self as Consumer>::assignment(self)
    }
}

// Implementation for rdkafka's BorrowedMessage
impl<'a> MessageExtractor for BorrowedMessage<'a> {
    fn topic(&self) -> &str {
        Message::topic(self)
    }

    fn partition(&self) -> i32 {
        Message::partition(self)
    }

    fn offset(&self) -> i64 {
        Message::offset(self)
    }

    fn timestamp_ms(&self) -> Option<i64> {
        Message::timestamp(self).to_millis()
    }

    fn key(&self) -> Option<&[u8]> {
        Message::key(self)
    }

    fn payload(&self) -> Option<&[u8]> {
        Message::payload(self)
    }
}
