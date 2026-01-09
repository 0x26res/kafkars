# Installation

## Requirements

- Python 3.10 or higher
- A running Kafka cluster

## Install from PyPI

```bash
pip install kafkars
```

## Install with extras

For development:

```bash
pip install kafkars[docs]
```

## Verify Installation

```python
import kafkars
print(kafkars.__version__)
```

## Dependencies

kafkars has minimal dependencies:

- **pyarrow** (>=14): For Arrow-based data transfer

The Rust core is compiled and bundled with the wheel, so no Rust toolchain is required for installation.
