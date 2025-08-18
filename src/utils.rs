use mongodb::{
    bson::{doc, Document},
    Client,
};

use crate::error::MongoAggregateError;

pub(crate) async fn load_view(
    client: &Client,
    collection_name: &str,
    view_id: &str,
) -> Result<Option<Document>, MongoAggregateError> {
    let collection = client
        .default_database()
        .expect("Default database not configured")
        .collection::<Document>(collection_name);
    Ok(collection
        .find_one(doc! { "view_id": view_id })
        .await
        .unwrap())
}

#[cfg(test)]
pub(crate) mod tests {
    use cqrs_es::doc::{Customer, CustomerEvent};
    use cqrs_es::persist::SerializedSnapshot;
    use cqrs_es::{persist::SerializedEvent, Aggregate, DomainEvent, EventEnvelope, View};
    use serde::{Deserialize, Serialize};

    #[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
    pub(crate) struct CustomerView {
        pub(crate) events: Vec<CustomerEvent>,
    }

    impl View<Customer> for CustomerView {
        fn update(&mut self, event: &EventEnvelope<Customer>) {
            self.events.push(event.payload.clone());
        }
    }

    pub(crate) fn test_event(id: &str, sequence: usize, event: CustomerEvent) -> SerializedEvent {
        let payload = serde_json::to_value(&event).unwrap();
        SerializedEvent {
            aggregate_id: id.to_string(),
            sequence,
            aggregate_type: Customer::aggregate_type().to_string(),
            event_type: event.event_type().to_string(),
            event_version: "1".to_string(),
            payload,
            metadata: Default::default(),
        }
    }

    pub(crate) fn test_snapshot_context(
        aggregate_id: String,
        aggregate: serde_json::Value,
        current_sequence: usize,
        current_snapshot: usize,
    ) -> SerializedSnapshot {
        SerializedSnapshot {
            aggregate_id,
            aggregate,
            current_sequence,
            current_snapshot,
        }
    }

    pub async fn mongodb_client() -> mongodb::Client {
        mongodb::Client::with_uri_str("mongodb://localhost:27017/test")
            .await
            .unwrap()
    }
}
