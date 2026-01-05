import pytest

from kafkars import SourceTopic, validate_source_topic


class TestValidateSourceTopic:
    def test_latest(self):
        validate_source_topic(SourceTopic.from_latest("my-topic"))

    def test_earliest(self):
        validate_source_topic(SourceTopic.from_earliest("my-topic"))

    def test_committed(self):
        validate_source_topic(SourceTopic.from_committed("my-topic"))

    def test_relative_time(self):
        validate_source_topic(SourceTopic.from_relative_time("my-topic", 3600000))

    def test_absolute_time(self):
        validate_source_topic(SourceTopic.from_absolute_time("my-topic", 1704067200000))

    def test_from_dict(self):
        validate_source_topic({"name": "my-topic", "policy": "latest"})

    def test_from_dict_with_time(self):
        validate_source_topic(
            {"name": "my-topic", "policy": "relative_time", "time_ms": 3600000}
        )

    def test_invalid_policy(self):
        with pytest.raises(ValueError, match="unknown policy"):
            validate_source_topic(SourceTopic(name="my-topic", policy="invalid"))

    def test_relative_time_missing_time_ms(self):
        with pytest.raises(ValueError, match="requires 'time_ms'"):
            validate_source_topic(SourceTopic(name="my-topic", policy="relative_time"))

    def test_absolute_time_missing_time_ms(self):
        with pytest.raises(ValueError, match="requires 'time_ms'"):
            validate_source_topic(SourceTopic(name="my-topic", policy="absolute_time"))

    def test_dict_missing_name(self):
        with pytest.raises(KeyError, match="name"):
            validate_source_topic({"policy": "latest"})

    def test_dict_missing_policy(self):
        with pytest.raises(KeyError, match="policy"):
            validate_source_topic({"name": "my-topic"})
