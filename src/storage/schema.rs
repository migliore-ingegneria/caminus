#![allow(clippy::collapsible_if)]

use crate::source::ChangeEvent;
use crate::storage::confluent::ConfluentRegistryClient;
use crate::storage::{StateStore, StorageError};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;
use tokio::sync::broadcast;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SchemaCompatibility {
    None,
    Backward,
    Forward,
    Full,
}

#[derive(Error, Debug)]
pub enum SchemaError {
    #[error("Storage error: {0}")]
    Storage(#[from] StorageError),
    #[error("Incompatible schema change: {0}")]
    Incompatibility(String),
    #[error("Validation failed: {0}")]
    Validation(String),
}

pub struct SchemaRegistry;

impl SchemaRegistry {
    /// Registers a new schema for a source. Validates compatibility with the existing schema if one exists.
    pub fn register_schema(
        store: &StateStore,
        source_id: &str,
        new_schema: Value,
        mode: SchemaCompatibility,
    ) -> Result<(), SchemaError> {
        if let Some(old_schema) = store.get_schema(source_id)? {
            Self::check_compatibility(&old_schema, &new_schema, mode)?;
        }
        store.save_schema(source_id, &new_schema)?;
        Ok(())
    }

    /// Evaluates if new_schema is compatible with old_schema based on the compatibility mode.
    pub fn check_compatibility(
        old_schema: &Value,
        new_schema: &Value,
        mode: SchemaCompatibility,
    ) -> Result<(), SchemaError> {
        if mode == SchemaCompatibility::None {
            return Ok(());
        }

        let old_fields = old_schema
            .get("fields")
            .and_then(|f| f.as_object())
            .ok_or_else(|| {
                SchemaError::Incompatibility("Old schema missing 'fields' object".to_string())
            })?;

        let new_fields = new_schema
            .get("fields")
            .and_then(|f| f.as_object())
            .ok_or_else(|| {
                SchemaError::Incompatibility("New schema missing 'fields' object".to_string())
            })?;

        // 1. Check for type changes in common fields
        for (field_name, old_type_val) in old_fields {
            if let Some(new_type_val) = new_fields.get(field_name) {
                if old_type_val != new_type_val {
                    return Err(SchemaError::Incompatibility(format!(
                        "Field '{}' type changed from {:?} to {:?}",
                        field_name, old_type_val, new_type_val
                    )));
                }
            }
        }

        match mode {
            SchemaCompatibility::Backward => {
                // Backward: New schema must be able to read old data.
                // We cannot delete fields because old data expects them.
                for field_name in old_fields.keys() {
                    if !new_fields.contains_key(field_name) {
                        return Err(SchemaError::Incompatibility(format!(
                            "Backward compatibility violation: Field '{}' was deleted",
                            field_name
                        )));
                    }
                }
            }
            SchemaCompatibility::Forward => {
                // Forward: Old schema must be able to read new data.
                // We cannot add new fields because old schema doesn't know about them.
                for field_name in new_fields.keys() {
                    if !old_fields.contains_key(field_name) {
                        return Err(SchemaError::Incompatibility(format!(
                            "Forward compatibility violation: New field '{}' added",
                            field_name
                        )));
                    }
                }
            }
            SchemaCompatibility::Full => {
                // Full: Both backward and forward compatible. No fields can be added or deleted.
                for field_name in old_fields.keys() {
                    if !new_fields.contains_key(field_name) {
                        return Err(SchemaError::Incompatibility(format!(
                            "Full compatibility violation: Field '{}' was deleted",
                            field_name
                        )));
                    }
                }
                for field_name in new_fields.keys() {
                    if !old_fields.contains_key(field_name) {
                        return Err(SchemaError::Incompatibility(format!(
                            "Full compatibility violation: Field '{}' was added",
                            field_name
                        )));
                    }
                }
            }
            SchemaCompatibility::None => {}
        }

        Ok(())
    }

    /// Validates a change event payload against its registered schema.
    pub fn validate_event(
        store: &StateStore,
        source_id: &str,
        event: &ChangeEvent,
    ) -> Result<(), SchemaError> {
        let schema = match store.get_schema(source_id)? {
            Some(s) => s,
            None => return Ok(()), // No schema registered, skip validation
        };

        let fields = schema
            .get("fields")
            .and_then(|f| f.as_object())
            .ok_or_else(|| {
                SchemaError::Validation("Registered schema missing 'fields' object".to_string())
            })?;

        // Only validate payloads for Create and Update mutations
        if event.operation != crate::source::Operation::Create
            && event.operation != crate::source::Operation::Update
        {
            return Ok(());
        }

        if let Some(payload) = &event.after {
            let payload_obj = payload.as_object().ok_or_else(|| {
                SchemaError::Validation("Event payload is not a JSON object".to_string())
            })?;

            for (field_name, type_val) in fields {
                let expected_type = type_val.as_str().ok_or_else(|| {
                    SchemaError::Validation(format!(
                        "Schema type for '{}' is not a string",
                        field_name
                    ))
                })?;

                let val = payload_obj.get(field_name).ok_or_else(|| {
                    SchemaError::Validation(format!("Missing required field '{}'", field_name))
                })?;

                match expected_type {
                    "integer" => {
                        if !val.is_number() || val.as_f64().unwrap().fract() != 0.0 {
                            return Err(SchemaError::Validation(format!(
                                "Field '{}' expected integer, found {:?}",
                                field_name, val
                            )));
                        }
                    }
                    "string" => {
                        if !val.is_string() {
                            return Err(SchemaError::Validation(format!(
                                "Field '{}' expected string, found {:?}",
                                field_name, val
                            )));
                        }
                    }
                    "boolean" => {
                        if !val.is_boolean() {
                            return Err(SchemaError::Validation(format!(
                                "Field '{}' expected boolean, found {:?}",
                                field_name, val
                            )));
                        }
                    }
                    "float" => {
                        if !val.is_number() {
                            return Err(SchemaError::Validation(format!(
                                "Field '{}' expected float, found {:?}",
                                field_name, val
                            )));
                        }
                    }
                    other => {
                        return Err(SchemaError::Validation(format!(
                            "Unsupported schema type: {}",
                            other
                        )));
                    }
                }
            }
            Ok(())
        } else {
            Err(SchemaError::Validation(
                "Event payload 'after' is missing".to_string(),
            ))
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaMigrationEvent {
    pub source_id: String,
    pub schema_id: u32,
    pub old_schema: Option<Value>,
    pub new_schema: Value,
    pub compatibility_mode: SchemaCompatibility,
    pub timestamp: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HandshakeResult {
    Accepted { schema_id: u32 },
    Rejected { reason: String, routed_to_dlq: bool },
}

pub struct SchemaMigrationHandshake {
    pub confluent_client: ConfluentRegistryClient,
    pub dlq: crate::resiliency::dlq::DeadLetterQueue,
    broadcast_tx: broadcast::Sender<SchemaMigrationEvent>,
}

impl SchemaMigrationHandshake {
    pub fn new(
        confluent_client: ConfluentRegistryClient,
        dlq: crate::resiliency::dlq::DeadLetterQueue,
    ) -> Self {
        let (broadcast_tx, _) = broadcast::channel(128);
        Self {
            confluent_client,
            dlq,
            broadcast_tx,
        }
    }

    pub fn subscribe(&self) -> broadcast::Receiver<SchemaMigrationEvent> {
        self.broadcast_tx.subscribe()
    }

    /// Negotiates schema version handshake, verifying compatibility rules (`BACKWARD`/`FULL`),
    /// routing breaking mutations to DLQ, registering schemas, and streaming async notifications.
    pub fn negotiate_handshake(
        &self,
        store: &StateStore,
        source_id: &str,
        new_schema: Value,
        mode: SchemaCompatibility,
    ) -> HandshakeResult {
        let old_schema = match store.get_schema(source_id) {
            Ok(s) => s,
            Err(e) => {
                return HandshakeResult::Rejected {
                    reason: format!("Failed to read existing schema: {}", e),
                    routed_to_dlq: false,
                };
            }
        };

        if let Some(ref old) = old_schema {
            if let Err(compat_err) = SchemaRegistry::check_compatibility(old, &new_schema, mode) {
                let reason = compat_err.to_string();
                let event = ChangeEvent {
                    id: format!("schema-mutation-{}", source_id),
                    source_database: source_id.to_string(),
                    source_table_or_collection: "schema_migrations".to_string(),
                    operation: crate::source::Operation::Update,
                    timestamp: Utc::now(),
                    key: serde_json::json!({ "source_id": source_id }),
                    before: old_schema.clone(),
                    after: Some(new_schema.clone()),
                    transaction_id: None,
                    offset: "0".to_string(),
                };
                let dlq_record = crate::resiliency::dlq::DlqRecord::new(
                    event,
                    reason.clone(),
                    "SchemaMigrationHandshake".to_string(),
                    1,
                );
                let _ = self.dlq.route_to_dlq(&dlq_record);

                return HandshakeResult::Rejected {
                    reason,
                    routed_to_dlq: true,
                };
            }
        }

        let schema_str = new_schema.to_string();
        let schema_id = self.confluent_client.register_schema(source_id, &schema_str);

        if let Err(e) = store.save_schema(source_id, &new_schema) {
            return HandshakeResult::Rejected {
                reason: format!("Failed to store schema: {}", e),
                routed_to_dlq: false,
            };
        }

        let migration_event = SchemaMigrationEvent {
            source_id: source_id.to_string(),
            schema_id,
            old_schema,
            new_schema,
            compatibility_mode: mode,
            timestamp: Utc::now(),
        };

        let _ = self.broadcast_tx.send(migration_event);

        HandshakeResult::Accepted { schema_id }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use serde_json::json;
    use std::fs;

    #[test]
    fn test_schema_registration_and_validation() {
        let test_path = "./data/test_schema_db";
        let _ = fs::remove_dir_all(test_path);
        let store = StateStore::new(test_path).unwrap();

        let schema = json!({
            "fields": {
                "id": "integer",
                "name": "string",
                "active": "boolean"
            }
        });

        // Register initial schema
        SchemaRegistry::register_schema(
            &store,
            "pg_users",
            schema.clone(),
            SchemaCompatibility::None,
        )
        .unwrap();

        // Valid event
        let event = ChangeEvent {
            id: "evt-1".into(),
            source_database: "db".into(),
            source_table_or_collection: "users".into(),
            operation: crate::source::Operation::Create,
            timestamp: Utc::now(),
            key: json!({ "id": 1 }),
            before: None,
            after: Some(json!({ "id": 1, "name": "John", "active": true })),
            transaction_id: None,
            offset: "1".into(),
        };
        assert!(SchemaRegistry::validate_event(&store, "pg_users", &event).is_ok());

        // Invalid event - missing active field
        let invalid_event_1 = ChangeEvent {
            after: Some(json!({ "id": 1, "name": "John" })),
            ..event.clone()
        };
        assert!(SchemaRegistry::validate_event(&store, "pg_users", &invalid_event_1).is_err());

        // Invalid event - type mismatch
        let invalid_event_2 = ChangeEvent {
            after: Some(json!({ "id": "not-an-int", "name": "John", "active": true })),
            ..event.clone()
        };
        assert!(SchemaRegistry::validate_event(&store, "pg_users", &invalid_event_2).is_err());

        let _ = fs::remove_dir_all(test_path);
    }

    #[test]
    fn test_schema_compatibility() {
        let old_schema = json!({
            "fields": {
                "id": "integer",
                "name": "string"
            }
        });

        // 1. Backward compatible: Adding a field is allowed
        let new_schema_add = json!({
            "fields": {
                "id": "integer",
                "name": "string",
                "email": "string"
            }
        });
        assert!(
            SchemaRegistry::check_compatibility(
                &old_schema,
                &new_schema_add,
                SchemaCompatibility::Backward
            )
            .is_ok()
        );

        // Backward incompatible: Deleting a field is not allowed
        let new_schema_del = json!({
            "fields": {
                "id": "integer"
            }
        });
        assert!(
            SchemaRegistry::check_compatibility(
                &old_schema,
                &new_schema_del,
                SchemaCompatibility::Backward
            )
            .is_err()
        );

        // 2. Forward compatible: Deleting a field is allowed
        assert!(
            SchemaRegistry::check_compatibility(
                &old_schema,
                &new_schema_del,
                SchemaCompatibility::Forward
            )
            .is_ok()
        );

        // Forward incompatible: Adding a field is not allowed
        assert!(
            SchemaRegistry::check_compatibility(
                &old_schema,
                &new_schema_add,
                SchemaCompatibility::Forward
            )
            .is_err()
        );

        // 3. Full compatibility
        assert!(
            SchemaRegistry::check_compatibility(
                &old_schema,
                &old_schema,
                SchemaCompatibility::Full
            )
            .is_ok()
        );
        assert!(
            SchemaRegistry::check_compatibility(
                &old_schema,
                &new_schema_add,
                SchemaCompatibility::Full
            )
            .is_err()
        );
    }

    #[tokio::test]
    async fn test_schema_migration_handshake_negotiation() {
        let test_path = "./data/test_handshake_db";
        let _ = fs::remove_dir_all(test_path);
        let store = StateStore::new(test_path).unwrap();

        let confluent_client = ConfluentRegistryClient::new("http://localhost:8081");
        let dlq = crate::resiliency::dlq::DeadLetterQueue::new("dlq_schema_topic".into());
        let handshake = SchemaMigrationHandshake::new(confluent_client, dlq);

        let mut receiver = handshake.subscribe();

        let initial_schema = json!({
            "fields": {
                "id": "integer",
                "name": "string"
            }
        });

        // 1. Initial schema handshake negotiation
        let res1 = handshake.negotiate_handshake(
            &store,
            "orders",
            initial_schema.clone(),
            SchemaCompatibility::Backward,
        );
        assert!(matches!(res1, HandshakeResult::Accepted { .. }));

        let event1 = receiver.recv().await.unwrap();
        assert_eq!(event1.source_id, "orders");
        assert_eq!(event1.schema_id, 100);

        // 2. Backward compatible modification (adding field)
        let compatible_schema = json!({
            "fields": {
                "id": "integer",
                "name": "string",
                "amount": "float"
            }
        });
        let res2 = handshake.negotiate_handshake(
            &store,
            "orders",
            compatible_schema.clone(),
            SchemaCompatibility::Backward,
        );
        assert!(matches!(res2, HandshakeResult::Accepted { .. }));

        let event2 = receiver.recv().await.unwrap();
        assert_eq!(event2.schema_id, 100);

        // 3. Backward incompatible modification (deleting field)
        let incompatible_schema = json!({
            "fields": {
                "id": "integer"
            }
        });
        let res3 = handshake.negotiate_handshake(
            &store,
            "orders",
            incompatible_schema,
            SchemaCompatibility::Backward,
        );
        assert!(matches!(
            res3,
            HandshakeResult::Rejected {
                routed_to_dlq: true,
                ..
            }
        ));

        let _ = fs::remove_dir_all(test_path);
    }
}
