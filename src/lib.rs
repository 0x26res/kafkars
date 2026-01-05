mod consumer;
mod source_topic;

use consumer::{KafkaMessage, RdKafkaConsumer};
use pyo3::prelude::*;
use std::collections::HashMap;

#[pyfunction]
fn hello() -> String {
    consumer::hello()
}

#[pyfunction]
fn consume_messages(config: HashMap<String, String>, topic: &str) -> PyResult<()> {
    let kafka_consumer = RdKafkaConsumer::new(config, topic)
        .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)?;

    println!(
        "Consuming messages from topic '{}' (press Ctrl+C to stop)...",
        topic
    );

    consumer::consume_messages(&kafka_consumer, |msg: &KafkaMessage| {
        let key = msg.key.as_deref().unwrap_or("null");
        let value = msg.value.as_deref().unwrap_or("null");
        println!("Key: {}, Value: {}", key, value);
    });

    Ok(())
}

/// A Python module implemented in Rust.
#[pymodule]
fn kafkars(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(hello, m)?)?;
    m.add_function(wrap_pyfunction!(consume_messages, m)?)?;
    Ok(())
}
