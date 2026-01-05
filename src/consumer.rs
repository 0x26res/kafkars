use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::message::Message;
use std::collections::HashMap;
use std::time::Duration;

#[derive(Debug, Clone, PartialEq)]
pub struct KafkaMessage {
    pub key: Option<String>,
    pub value: Option<String>,
}

pub trait MessageConsumer {
    fn poll(&self) -> Option<Result<KafkaMessage, String>>;
}

pub struct RdKafkaConsumer {
    consumer: BaseConsumer,
}

impl RdKafkaConsumer {
    pub fn new(config: HashMap<String, String>, topic: &str) -> Result<Self, String> {
        let mut client_config = ClientConfig::new();
        for (key, value) in &config {
            client_config.set(key, value);
        }
        let consumer: BaseConsumer = client_config.create().map_err(|e| e.to_string())?;

        consumer.subscribe(&[topic]).map_err(|e| e.to_string())?;

        Ok(Self { consumer })
    }
}

impl MessageConsumer for RdKafkaConsumer {
    fn poll(&self) -> Option<Result<KafkaMessage, String>> {
        match self.consumer.poll(Duration::from_millis(1000)) {
            Some(Ok(msg)) => {
                let key = msg.key().map(|k| String::from_utf8_lossy(k).to_string());
                let value = msg
                    .payload()
                    .map(|v| String::from_utf8_lossy(v).to_string());
                Some(Ok(KafkaMessage { key, value }))
            }
            Some(Err(e)) => Some(Err(e.to_string())),
            None => None,
        }
    }
}

pub fn hello() -> String {
    "Hello from kafkars!".to_string()
}

pub fn consume_messages<C: MessageConsumer, F: FnMut(&KafkaMessage)>(
    consumer: &C,
    mut on_message: F,
) {
    loop {
        match consumer.poll() {
            Some(Ok(msg)) => on_message(&msg),
            Some(Err(e)) => eprintln!("Error consuming message: {}", e),
            None => {}
        }
    }
}

#[allow(dead_code)]
pub fn consume_messages_with_limit<C: MessageConsumer, F: FnMut(&KafkaMessage)>(
    consumer: &C,
    limit: usize,
    mut on_message: F,
) -> usize {
    let mut count = 0;
    while count < limit {
        match consumer.poll() {
            Some(Ok(msg)) => {
                on_message(&msg);
                count += 1;
            }
            Some(Err(e)) => eprintln!("Error consuming message: {}", e),
            None => {}
        }
    }
    count
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;

    struct MockConsumer {
        messages: RefCell<Vec<Result<KafkaMessage, String>>>,
    }

    impl MockConsumer {
        fn new(messages: Vec<Result<KafkaMessage, String>>) -> Self {
            Self {
                messages: RefCell::new(messages.into_iter().rev().collect()),
            }
        }
    }

    impl MessageConsumer for MockConsumer {
        fn poll(&self) -> Option<Result<KafkaMessage, String>> {
            self.messages.borrow_mut().pop()
        }
    }

    #[test]
    fn test_consume_messages_with_limit() {
        let messages = vec![
            Ok(KafkaMessage {
                key: Some("key1".to_string()),
                value: Some("value1".to_string()),
            }),
            Ok(KafkaMessage {
                key: Some("key2".to_string()),
                value: Some("value2".to_string()),
            }),
            Ok(KafkaMessage {
                key: None,
                value: Some("value3".to_string()),
            }),
        ];

        let consumer = MockConsumer::new(messages);
        let mut received: Vec<KafkaMessage> = Vec::new();

        let count = consume_messages_with_limit(&consumer, 3, |msg| {
            received.push(msg.clone());
        });

        assert_eq!(count, 3);
        assert_eq!(received.len(), 3);
        assert_eq!(received[0].key, Some("key1".to_string()));
        assert_eq!(received[0].value, Some("value1".to_string()));
        assert_eq!(received[1].key, Some("key2".to_string()));
        assert_eq!(received[2].key, None);
    }

    #[test]
    fn test_hello() {
        assert_eq!(hello(), "Hello from kafkars!");
    }
}
