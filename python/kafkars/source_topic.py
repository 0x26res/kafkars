from dataclasses import dataclass


@dataclass
class SourceTopic:
    """Configuration for a Kafka source topic."""

    name: str
    policy: str = "latest"
    time_ms: int | None = None

    @staticmethod
    def from_latest(name: str) -> "SourceTopic":
        return SourceTopic(name=name, policy="latest")

    @staticmethod
    def from_earliest(name: str) -> "SourceTopic":
        return SourceTopic(name=name, policy="earliest")

    @staticmethod
    def from_committed(name: str) -> "SourceTopic":
        return SourceTopic(name=name, policy="committed")

    @staticmethod
    def from_relative_time(name: str, time_ms: int) -> "SourceTopic":
        return SourceTopic(name=name, policy="relative_time", time_ms=time_ms)

    @staticmethod
    def from_absolute_time(name: str, time_ms: int) -> "SourceTopic":
        return SourceTopic(name=name, policy="absolute_time", time_ms=time_ms)
