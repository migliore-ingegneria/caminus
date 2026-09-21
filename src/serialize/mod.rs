use crate::source::ChangeEvent;
use thiserror::Error;

pub mod avro;

#[derive(Error, Debug)]
pub enum SerializationError {
    #[error("simd-json serialization error: {0}")]
    Simd(#[from] simd_json::Error),
}

/// Serializes a ChangeEvent utilizing SIMD-accelerated simd-json engine.
pub fn serialize_event(event: &ChangeEvent) -> Result<String, SerializationError> {
    let json_str = simd_json::to_string(event)?;
    Ok(json_str)
}

std::thread_local! {
    static SCRATCH_BUFFER: std::cell::RefCell<Vec<u8>> = std::cell::RefCell::new(Vec::with_capacity(8192));
}

/// Zero-copy SIMD serialization utilizing pre-allocated thread-local scratchpad buffer.
pub fn serialize_event_zero_copy(event: &ChangeEvent) -> Result<Vec<u8>, SerializationError> {
    SCRATCH_BUFFER.with(|buf| {
        let mut b = buf.borrow_mut();
        b.clear();
        simd_json::to_writer(&mut *b, event)?;
        Ok(b.clone())
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::source::Operation;
    use chrono::Utc;

    #[test]
    fn test_simd_serialization() {
        let event = ChangeEvent {
            id: "evt-simd".into(),
            source_database: "db".into(),
            source_table_or_collection: "users".into(),
            operation: Operation::Create,
            timestamp: Utc::now(),
            key: serde_json::json!({ "id": 1 }),
            before: None,
            after: Some(serde_json::json!({ "id": 1, "name": "Simd" })),
            transaction_id: None,
            offset: "100".into(),
        };

        let serialized = serialize_event(&event).expect("Failed to serialize event");
        assert!(serialized.contains("evt-simd"));
        assert!(serialized.contains("users"));
    }

    #[test]
    fn test_simd_zero_copy_serialization() {
        let event = ChangeEvent {
            id: "evt-zero-copy".into(),
            source_database: "db".into(),
            source_table_or_collection: "users".into(),
            operation: Operation::Create,
            timestamp: Utc::now(),
            key: serde_json::json!({ "id": 2 }),
            before: None,
            after: Some(serde_json::json!({ "id": 2, "name": "ZeroCopy" })),
            transaction_id: None,
            offset: "101".into(),
        };

        let bytes = serialize_event_zero_copy(&event).expect("Failed zero-copy serialization");
        let json_str = String::from_utf8(bytes).unwrap();
        assert!(json_str.contains("evt-zero-copy"));
        assert!(json_str.contains("ZeroCopy"));
    }
}
