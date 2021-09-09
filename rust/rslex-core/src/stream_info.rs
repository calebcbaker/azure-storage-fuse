use crate::{
    value_with_eq::{hash_sync_record, sync_record_eq},
    SyncRecord,
};
use derivative::Derivative;
use std::{
    any::Any,
    borrow::Cow,
    collections::HashMap,
    fmt::{Display, Error, Formatter},
    hash::{Hash, Hasher},
    sync::Arc,
};

pub type SessionProperties = HashMap<String, Arc<dyn Any + Send + Sync>>;

/// A reference to a resource that can be accessed as a stream of bytes.
///
/// StreamInfos can be accessed by calling methods on a [`StreamAccessor`](crate::file_io::StreamAccessor). The stream accessor delegates
/// its work to implementations of [`StreamHandler`](crate::file_io::StreamHandler). Which handler to use for a specific StreamInfo is determined
/// by the [`handler`](StreamInfo::handler) field.
#[derive(Clone, Debug, Derivative)]
#[derivative(PartialOrd)]
pub struct StreamInfo {
    handler: Cow<'static, str>,
    resource_id: String,
    arguments: SyncRecord,
    #[derivative(PartialOrd = "ignore")]
    session_properties: SessionProperties,
}

impl Eq for StreamInfo {}

impl PartialEq for StreamInfo {
    fn eq(&self, other: &Self) -> bool {
        self.handler == other.handler && self.resource_id == other.resource_id && sync_record_eq(&self.arguments, &other.arguments)
    }
}

impl Hash for StreamInfo {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.handler.hash(state);
        self.resource_id.hash(state);
        hash_sync_record(&self.arguments, state);
    }
}

impl StreamInfo {
    pub fn new(handler: impl Into<Cow<'static, str>>, resource_id: impl Into<String>, arguments: SyncRecord) -> StreamInfo {
        StreamInfo {
            handler: handler.into(),
            resource_id: resource_id.into(),
            arguments,
            session_properties: HashMap::new(),
        }
    }

    pub fn with_session_properties(mut self, session_properties: SessionProperties) -> Self {
        self.session_properties = session_properties;
        self
    }

    /// The handler type that can process this stream.
    pub fn handler(&self) -> &str {
        &self.handler
    }

    /// The resource that this stream references.
    pub fn resource_id(&self) -> &str {
        &self.resource_id
    }

    /// Arguments required to access the resource.
    pub fn arguments(&self) -> &SyncRecord {
        &self.arguments
    }

    /// Properties which are only relevant at runtime. They should not be serialized.
    pub fn session_properties(&self) -> &SessionProperties {
        &self.session_properties
    }
}

impl Display for StreamInfo {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        f.write_fmt(format_args!(
            "{{Handler: \"{}\", ResourceId: \"{}\", Arguments: {}}}",
            self.handler, self.resource_id, self.arguments
        ))
    }
}

#[cfg(test)]
mod tests {
    use crate::{session_properties_ext::SessionPropertiesExt, sync_record, StreamInfo, SyncRecord};
    use std::collections::HashMap;

    #[test]
    fn eq_for_equal_stream_infos_returns_true() {
        let s1 = StreamInfo::new("H", "R", SyncRecord::empty());
        let s2 = StreamInfo::new("H", "R", SyncRecord::empty());
        assert_eq!(s1, s2);
    }

    #[test]
    fn eq_when_handler_is_different_returns_false() {
        let s1 = StreamInfo::new("H", "R", SyncRecord::empty());
        let s2 = StreamInfo::new("H2", "R", SyncRecord::empty());
        assert_ne!(s1, s2);
    }

    #[test]
    fn eq_when_resource_id_is_different_returns_false() {
        let s1 = StreamInfo::new("H", "R", SyncRecord::empty());
        let s2 = StreamInfo::new("H", "R2", SyncRecord::empty());
        assert_ne!(s1, s2);
    }

    #[test]
    fn eq_when_arguments_are_different_returns_false() {
        let s1 = StreamInfo::new("H", "R", SyncRecord::empty());
        let s2 = StreamInfo::new("H", "R", sync_record! { "A" => 1.0 });
        assert_ne!(s1, s2);
    }

    #[test]
    fn eq_when_session_properties_are_different_returns_true() {
        let s1 = StreamInfo::new("H", "R", SyncRecord::empty());
        let s2 = StreamInfo::new("H", "R", SyncRecord::empty()).with_session_properties(HashMap::new().with_size(123));
        assert_eq!(s1, s2);
    }
}
