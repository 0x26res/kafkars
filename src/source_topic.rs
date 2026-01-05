#[derive(Debug, Clone, PartialEq)]
pub enum OffsetPolicy {
    Latest,
    Earliest,
    Committed,
    RelativeTime { ms: i64 },
    AbsoluteTime { ms: i64 },
    StartOfDay { time_ms: i64, timezone: String },
}

#[derive(Debug, Clone)]
pub struct SourceTopic {
    pub name: String,
    pub offset_policy: OffsetPolicy,
}

impl SourceTopic {
    pub fn from_latest(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            offset_policy: OffsetPolicy::Latest,
        }
    }

    pub fn from_earliest(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            offset_policy: OffsetPolicy::Earliest,
        }
    }

    pub fn from_committed(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            offset_policy: OffsetPolicy::Committed,
        }
    }

    pub fn from_relative_time(name: impl Into<String>, ms: i64) -> Self {
        Self {
            name: name.into(),
            offset_policy: OffsetPolicy::RelativeTime { ms },
        }
    }

    pub fn from_absolute_time(name: impl Into<String>, ms: i64) -> Self {
        Self {
            name: name.into(),
            offset_policy: OffsetPolicy::AbsoluteTime { ms },
        }
    }

    pub fn from_start_of_day(
        name: impl Into<String>,
        time_ms: i64,
        timezone: impl Into<String>,
    ) -> Self {
        Self {
            name: name.into(),
            offset_policy: OffsetPolicy::StartOfDay {
                time_ms,
                timezone: timezone.into(),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_from_latest() {
        let topic = SourceTopic::from_latest("my-topic");
        assert_eq!(topic.name, "my-topic");
        assert_eq!(topic.offset_policy, OffsetPolicy::Latest);
    }

    #[test]
    fn test_from_relative_time() {
        let topic = SourceTopic::from_relative_time("my-topic", 60_000);
        assert_eq!(topic.name, "my-topic");
        assert_eq!(
            topic.offset_policy,
            OffsetPolicy::RelativeTime { ms: 60_000 }
        );
    }

    #[test]
    fn test_from_start_of_day() {
        let topic = SourceTopic::from_start_of_day("my-topic", 3600_000, "UTC");
        assert_eq!(topic.name, "my-topic");
        assert_eq!(
            topic.offset_policy,
            OffsetPolicy::StartOfDay {
                time_ms: 3600_000,
                timezone: "UTC".to_string()
            }
        );
    }

    #[test]
    fn test_from_earliest() {
        let topic = SourceTopic::from_earliest("my-topic");
        assert_eq!(topic.name, "my-topic");
        assert_eq!(topic.offset_policy, OffsetPolicy::Earliest);
    }

    #[test]
    fn test_from_committed() {
        let topic = SourceTopic::from_committed("my-topic");
        assert_eq!(topic.name, "my-topic");
        assert_eq!(topic.offset_policy, OffsetPolicy::Committed);
    }

    #[test]
    fn test_from_absolute_time() {
        let topic = SourceTopic::from_absolute_time("my-topic", 1704067200000);
        assert_eq!(topic.name, "my-topic");
        assert_eq!(
            topic.offset_policy,
            OffsetPolicy::AbsoluteTime { ms: 1704067200000 }
        );
    }
}
