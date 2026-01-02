use pyo3::prelude::*;

/// Returns a greeting message.
#[pyfunction]
fn hello() -> String {
    "Hello from kafkars!".to_string()
}

/// A Python module implemented in Rust.
#[pymodule]
fn kafkars(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(hello, m)?)?;
    Ok(())
}
