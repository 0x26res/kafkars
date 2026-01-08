pub mod consumer_manager;
pub mod source_topic;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use arrow::array::{
    BinaryArray, BooleanArray, Int32Array, Int64Array, RecordBatch, StringArray,
    TimestampMillisecondArray,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::pyarrow::ToPyArrow;
use consumer_manager::ConsumerManager;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use source_topic::SourceTopic;

struct PySourceTopic(SourceTopic);

impl<'py> FromPyObject<'py> for PySourceTopic {
    fn extract_bound(obj: &Bound<'py, PyAny>) -> PyResult<Self> {
        let (name, policy, time_ms): (String, String, Option<i64>) = if let Ok(dict) =
            obj.downcast::<PyDict>()
        {
            let name: String = dict
                .get_item("name")?
                .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyKeyError, _>("missing 'name' key"))?
                .extract()?;
            let policy: String = dict
                .get_item("policy")?
                .ok_or_else(|| {
                    PyErr::new::<pyo3::exceptions::PyKeyError, _>("missing 'policy' key")
                })?
                .extract()?;
            let time_ms: Option<i64> =
                dict.get_item("time_ms")?.map(|v| v.extract()).transpose()?;
            (name, policy, time_ms)
        } else {
            let name: String = obj.getattr("name")?.extract()?;
            let policy: String = obj.getattr("policy")?.extract()?;
            let time_ms: Option<i64> = obj.getattr("time_ms")?.extract()?;
            (name, policy, time_ms)
        };

        let source_topic = match policy.as_str() {
            "latest" => SourceTopic::from_latest(name),
            "earliest" => SourceTopic::from_earliest(name),
            "committed" => SourceTopic::from_committed(name),
            "relative_time" => {
                let ms = time_ms.ok_or_else(|| {
                    PyErr::new::<pyo3::exceptions::PyValueError, _>(
                        "relative_time policy requires 'time_ms'",
                    )
                })?;
                SourceTopic::from_relative_time(name, ms)
            }
            "absolute_time" => {
                let ms = time_ms.ok_or_else(|| {
                    PyErr::new::<pyo3::exceptions::PyValueError, _>(
                        "absolute_time policy requires 'time_ms'",
                    )
                })?;
                SourceTopic::from_absolute_time(name, ms)
            }
            _ => {
                return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                    "unknown policy: '{}'. Expected: latest, earliest, committed, relative_time, absolute_time",
                    policy
                )))
            }
        };

        Ok(PySourceTopic(source_topic))
    }
}

#[pyfunction]
fn validate_source_topic(topic: PySourceTopic) -> PyResult<()> {
    let _ = topic.0;
    Ok(())
}

fn timestamp_ms_type() -> DataType {
    DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into()))
}

fn message_schema() -> Schema {
    Schema::new(vec![
        Field::new("key", DataType::Binary, true),
        Field::new("value", DataType::Binary, true),
        Field::new("topic", DataType::Utf8, false),
        Field::new("partition", DataType::Int32, false),
        Field::new("offset", DataType::Int64, false),
        Field::new("timestamp", timestamp_ms_type(), false),
    ])
}

fn partition_state_schema() -> Schema {
    Schema::new(vec![
        Field::new("topic", DataType::Utf8, false),
        Field::new("partition", DataType::Int32, false),
        Field::new("replay_start_offset", DataType::Int64, false),
        Field::new("replay_end_offset", DataType::Int64, false),
        Field::new("consumed_offset", DataType::Int64, false),
        Field::new("released_offset", DataType::Int64, false),
        Field::new("last_message_timestamp", timestamp_ms_type(), true),
        Field::new("cutoff", timestamp_ms_type(), false),
        Field::new("is_live", DataType::Boolean, false),
        Field::new("is_paused", DataType::Boolean, false),
    ])
}

#[pyclass]
struct PyConsumerManager {
    manager: ConsumerManager,
    message_schema: Arc<Schema>,
    partition_state_schema: Arc<Schema>,
}

#[pymethods]
impl PyConsumerManager {
    #[new]
    fn new(
        config: HashMap<String, String>,
        topics: Vec<PySourceTopic>,
        cutoff_ms: i64,
        batch_size: usize,
    ) -> PyResult<Self> {
        let source_topics: Vec<SourceTopic> = topics.into_iter().map(|t| t.0).collect();

        let manager = ConsumerManager::create(config, source_topics, cutoff_ms, batch_size)
            .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)?;

        Ok(Self {
            manager,
            message_schema: Arc::new(message_schema()),
            partition_state_schema: Arc::new(partition_state_schema()),
        })
    }

    fn poll<'py>(&mut self, py: Python<'py>, timeout_ms: u64) -> PyResult<Bound<'py, PyAny>> {
        let messages = self
            .manager
            .poll(Duration::from_millis(timeout_ms))
            .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)?;

        let keys: Vec<Option<&[u8]>> = messages.iter().map(|m| m.key.as_deref()).collect();
        let values: Vec<Option<&[u8]>> = messages.iter().map(|m| m.value.as_deref()).collect();
        let topics: Vec<&str> = messages.iter().map(|m| m.topic.as_str()).collect();
        let partitions: Vec<i32> = messages.iter().map(|m| m.partition).collect();
        let offsets: Vec<i64> = messages.iter().map(|m| m.offset).collect();
        let timestamps: Vec<i64> = messages.iter().map(|m| m.timestamp_ms).collect();

        let batch = RecordBatch::try_new(
            self.message_schema.clone(),
            vec![
                Arc::new(BinaryArray::from(keys)),
                Arc::new(BinaryArray::from(values)),
                Arc::new(StringArray::from(topics)),
                Arc::new(Int32Array::from(partitions)),
                Arc::new(Int64Array::from(offsets)),
                Arc::new(TimestampMillisecondArray::from(timestamps).with_timezone("UTC")),
            ],
        )
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;

        batch
            .to_pyarrow(py)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))
    }

    fn is_live(&self) -> bool {
        self.manager.is_live()
    }

    fn get_priming_watermark(&self) -> Option<i64> {
        self.manager.get_priming_watermark()
    }

    fn held_message_count(&self) -> usize {
        self.manager.held_message_count()
    }

    fn paused_partition_count(&self) -> usize {
        self.manager.paused_partition_count()
    }

    fn partition_state<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let partition_info = self.manager.partition_info();
        let replay_offsets_map = self.manager.start_offsets();
        let cutoff = self.manager.cutoff_ms();

        let mut sorted_partitions: Vec<_> = partition_info.values().collect();
        sorted_partitions.sort_by(|a, b| (&a.topic, a.partition).cmp(&(&b.topic, b.partition)));

        let topics: Vec<&str> = sorted_partitions.iter().map(|p| p.topic.as_str()).collect();
        let partition_ids: Vec<i32> = sorted_partitions.iter().map(|p| p.partition).collect();
        let replay_start_offsets: Vec<i64> = sorted_partitions
            .iter()
            .map(|p| {
                replay_offsets_map
                    .get_replay_start_offset(&p.topic, p.partition)
                    .unwrap_or(0)
            })
            .collect();
        let replay_end_offsets: Vec<i64> = sorted_partitions
            .iter()
            .map(|p| {
                replay_offsets_map
                    .get_replay_end_offset(&p.topic, p.partition)
                    .unwrap_or(0)
            })
            .collect();
        let consumed_offsets: Vec<i64> = sorted_partitions
            .iter()
            .map(|p| p.consumed_offset)
            .collect();
        let released_offsets: Vec<i64> = sorted_partitions
            .iter()
            .map(|p| p.released_offset)
            .collect();
        let last_timestamps: Vec<Option<i64>> =
            sorted_partitions.iter().map(|p| p.timestamp_ms).collect();
        let cutoffs: Vec<i64> = sorted_partitions.iter().map(|_| cutoff).collect();
        let is_live: Vec<bool> = sorted_partitions.iter().map(|p| p.is_live).collect();
        let is_paused: Vec<bool> = sorted_partitions.iter().map(|p| p.is_paused).collect();

        let batch = RecordBatch::try_new(
            self.partition_state_schema.clone(),
            vec![
                Arc::new(StringArray::from(topics)),
                Arc::new(Int32Array::from(partition_ids)),
                Arc::new(Int64Array::from(replay_start_offsets)),
                Arc::new(Int64Array::from(replay_end_offsets)),
                Arc::new(Int64Array::from(consumed_offsets)),
                Arc::new(Int64Array::from(released_offsets)),
                Arc::new(TimestampMillisecondArray::from(last_timestamps).with_timezone("UTC")),
                Arc::new(TimestampMillisecondArray::from(cutoffs).with_timezone("UTC")),
                Arc::new(BooleanArray::from(is_live)),
                Arc::new(BooleanArray::from(is_paused)),
            ],
        )
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;

        batch
            .to_pyarrow(py)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))
    }
}

#[pyfunction]
fn get_message_schema(py: Python<'_>) -> PyResult<Py<PyAny>> {
    let schema = message_schema();
    schema
        .to_pyarrow(py)
        .map(|bound| bound.unbind())
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))
}

#[pyfunction]
fn get_partition_state_schema(py: Python<'_>) -> PyResult<Py<PyAny>> {
    let schema = partition_state_schema();
    schema
        .to_pyarrow(py)
        .map(|bound| bound.unbind())
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))
}

/// Version from Cargo.toml, set at compile time.
const VERSION: &str = env!("CARGO_PKG_VERSION");

#[pymodule]
fn _lib(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("__version__", VERSION)?;
    m.add_class::<PyConsumerManager>()?;
    m.add_function(wrap_pyfunction!(validate_source_topic, m)?)?;
    m.add_function(wrap_pyfunction!(get_message_schema, m)?)?;
    m.add_function(wrap_pyfunction!(get_partition_state_schema, m)?)?;
    Ok(())
}
