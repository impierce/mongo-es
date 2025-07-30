use async_trait::async_trait;
use cqrs_es::{
    persist::{
        PersistedEventRepository, PersistenceError, ReplayStream, SerializedEvent,
        SerializedSnapshot,
    },
    Aggregate,
};
use futures::StreamExt;
use mongodb::{
    bson::{doc, Document},
    Cursor,
};
use mongodb::{options::FindOptions, Client, Collection};
use serde_json::Value;

use crate::error::MongoAggregateError;

const DEFAULT_EVENT_COLLECTION: &str = "events";
const DEFAULT_SNAPSHOT_COLLECTION: &str = "snapshots";

const DEFAULT_STREAMING_CHANNEL_SIZE: usize = 100;

pub struct MongoEventRepository {
    client: Client,
    event_collection: String,
    snapshot_collection: String,
    stream_channel_size: usize,
}

impl MongoEventRepository {
    pub fn new(client: Client) -> Self {
        Self::use_collection_names(
            client,
            DEFAULT_EVENT_COLLECTION,
            DEFAULT_SNAPSHOT_COLLECTION,
        )
    }

    pub fn with_streaming_channel_size(self, stream_channel_size: usize) -> Self {
        Self {
            client: self.client,
            event_collection: self.event_collection,
            snapshot_collection: self.snapshot_collection,
            stream_channel_size,
        }
    }

    fn use_collection_names(
        client: Client,
        event_collection: &str,
        snapshot_collection: &str,
    ) -> Self {
        Self {
            client,
            event_collection: event_collection.to_string(),
            snapshot_collection: snapshot_collection.to_string(),
            stream_channel_size: DEFAULT_STREAMING_CHANNEL_SIZE,
        }
    }

    pub(crate) async fn insert_events(
        &self,
        events: &[SerializedEvent],
    ) -> Result<(), PersistenceError> {
        if events.is_empty() {
            return Ok(());
        }

        let collection: Collection<Document> = self
            .client
            .database("my_db")
            .collection(&self.event_collection);

        let documents: Vec<Document> = events
            .iter()
            .map(|event| {
                let mut doc = Document::new();
                doc.insert("aggregate_id", event.aggregate_id.clone());
                doc.insert("sequence", event.sequence.to_string());
                doc.insert("aggregate_type", event.aggregate_type.clone());
                doc.insert("event_type", event.event_type.clone());
                doc.insert("event_version", event.event_version.clone());
                doc.insert("payload", event.payload.to_string());
                doc.insert("metadata", event.metadata.to_string());
                doc
            })
            .collect();

        let res = collection.insert_many(documents).await.unwrap();

        println!(
            "Inserted {} events into `{}` collection",
            res.inserted_ids.len(),
            self.event_collection
        );

        Ok(())
    }

    /// Returns all events for the given aggregate type and id.
    // async fn query_events(
    //     &self,
    //     aggregate_type: &str,
    //     aggregate_id: &str,
    // ) -> Result<Vec<SerializedEvent>, MongoAggregateError> {
    //     let mut cursor = self
    //         .query_collection(aggregate_type, aggregate_id, &self.event_collection, 0)
    //         .await?;
    //     let mut events: Vec<SerializedEvent> = Default::default();
    //     while cursor.advance().await? {
    //         let document = cursor.deserialize_current()?;
    //         events.push(serialized_event(&document)?);
    //     }
    //     Ok(events)
    // }

    /// Returns all events for the given aggregate type and id, starting from the specified sequence.
    async fn query_events(
        &self,
        aggregate_type: &str,
        aggregate_id: &str,
        min_sequence: usize,
    ) -> Result<Vec<SerializedEvent>, MongoAggregateError> {
        let mut cursor = self
            .query_collection(
                aggregate_type,
                aggregate_id,
                &self.event_collection,
                min_sequence as i64,
            )
            .await?;
        let mut events: Vec<SerializedEvent> = Default::default();
        while cursor.advance().await? {
            let document = cursor.deserialize_current()?;
            events.push(serialized_event(&document)?);
        }
        Ok(events)
    }

    pub(crate) async fn update_snapshot<A: Aggregate>(
        &self,
        aggregate_payload: Value,
        aggregate_id: String,
        current_snapshot: usize,
        events: &[SerializedEvent],
    ) -> Result<(), MongoAggregateError> {
        let expected_snapshot = current_snapshot - 1;

        let collection: Collection<Document> = self
            .client
            .database("my_db")
            .collection(&self.snapshot_collection);

        let mut doc = Document::new();
        doc.insert("aggregate_id", &aggregate_id);
        doc.insert("aggregate_type", A::aggregate_type());
        doc.insert("payload", aggregate_payload.to_string());
        doc.insert("current_sequence", 0);
        doc.insert("current_snapshot", current_snapshot as i64);

        let res = collection.insert_one(doc).await.unwrap();

        println!(
            "Inserted snapshot for `{}` with id `{}`",
            A::aggregate_type(),
            &aggregate_id
        );

        Ok(())
    }

    /// Queries the MongoDB collection and returns a cursor.
    async fn query_collection(
        &self,
        aggregate_type: &str,
        aggregate_id: &str,
        collection: &str,
        min_sequence: i64,
    ) -> Result<Cursor<Document>, MongoAggregateError> {
        let filter = self.build_filter(aggregate_type, aggregate_id, min_sequence);
        let collection = self
            .client
            .database("my_db")
            .collection::<Document>(collection);
        let cursor = collection.find(filter).await?;
        Ok(cursor)
    }

    fn build_filter(
        &self,
        aggregate_type: &str,
        aggregate_id: &str,
        min_sequence: i64,
    ) -> Document {
        if min_sequence == 0 {
            return doc! {
                "aggregate_type": aggregate_type,
                "aggregate_id": aggregate_id,
            };
        } else {
            return doc! {
                "aggregate_type": aggregate_type,
                "aggregate_id": aggregate_id,
                "sequence": { "$gte": min_sequence },
            };
        }
    }
}

fn serialized_event(document: &Document) -> Result<SerializedEvent, MongoAggregateError> {
    let aggregate_id = document.get_str("aggregate_id")?.to_string();
    Ok(SerializedEvent {
        aggregate_id,
        sequence: 0,
        aggregate_type: "".to_string(),
        event_type: "".to_string(),
        event_version: "1".to_string(),
        payload: serde_json::json!({}),
        metadata: serde_json::json!({}),
    })
}

#[async_trait]
impl PersistedEventRepository for MongoEventRepository {
    async fn get_events<A: Aggregate>(
        &self,
        aggregate_id: &str,
    ) -> Result<Vec<SerializedEvent>, PersistenceError> {
        let events = self
            .query_events(&A::aggregate_type(), aggregate_id, 0)
            .await?;
        Ok(events)
    }

    async fn get_last_events<A: Aggregate>(
        &self,
        aggregate_id: &str,
        last_sequence: usize,
    ) -> Result<Vec<SerializedEvent>, PersistenceError> {
        let events = self
            .query_events(&A::aggregate_type(), aggregate_id, last_sequence)
            .await?;
        Ok(events)
    }

    async fn get_snapshot<A: Aggregate>(
        &self,
        aggregate_id: &str,
    ) -> Result<Option<SerializedSnapshot>, PersistenceError> {
        let mut cursor = self
            .query_collection(
                &A::aggregate_type(),
                aggregate_id,
                &self.snapshot_collection,
                0,
            )
            .await?;

        if let Some(result) = cursor.next().await {
            let document = result.map_err(MongoAggregateError::from)?;
            let payload = document
                .get_str("payload")
                .map_err(MongoAggregateError::from)?;
            println!(
                "Found snapshot for `{}` with id `{}`",
                A::aggregate_type(),
                aggregate_id
            );
            Ok(Some(SerializedSnapshot {
                aggregate_id: aggregate_id.to_string(),
                aggregate: serde_json::from_str(payload)?,
                current_sequence: document
                    .get_i64("current_sequence")
                    .map_err(MongoAggregateError::from)? as usize,
                current_snapshot: document
                    .get_i64("current_snapshot")
                    .map_err(MongoAggregateError::from)? as usize,
            }))
        } else {
            Ok(None)
        }
    }

    async fn persist<A: Aggregate>(
        &self,
        events: &[SerializedEvent],
        snapshot_update: Option<(String, Value, usize)>,
    ) -> Result<(), PersistenceError> {
        match snapshot_update {
            None => {
                self.insert_events(events).await?;
            }
            Some((aggregate_id, aggregate_payload, current_snapshot)) => {
                self.update_snapshot::<A>(
                    aggregate_payload,
                    aggregate_id,
                    current_snapshot,
                    events,
                )
                .await?;
            }
        }
        Ok(())
    }

    async fn stream_events<A: Aggregate>(
        &self,
        aggregate_id: &str,
    ) -> Result<ReplayStream, PersistenceError> {
        Ok(ReplayStream::new(1).1)
    }

    // https://github.com/serverlesstechnology/postgres-es/blob/main/src/event_repository.rs#L99C5-L100C96
    // TODO: aggregate id is unused here, `stream_events` function needs to be broken up
    async fn stream_all_events<A: Aggregate>(&self) -> Result<ReplayStream, PersistenceError> {
        Ok(ReplayStream::new(1).1)
    }
}

#[cfg(test)]
mod tests {
    use cqrs_es::doc::{Customer, CustomerEvent};
    use cqrs_es::persist::{PersistedEventRepository, SerializedEvent};
    use cqrs_es::{Aggregate, DomainEvent};
    use serde::{Deserialize, Serialize};

    use crate::error::MongoAggregateError;
    use crate::test_utils::tests::{mongodb_client, test_event, test_snapshot_context};
    use crate::MongoEventRepository;

    #[tokio::test]
    async fn test_event_repository() {
        let client = mongodb_client().await;
        let repository = MongoEventRepository::new(client).with_streaming_channel_size(1);
        let aggregate_id = uuid::Uuid::new_v4().to_string();
        let events = repository
            .get_events::<Customer>(&aggregate_id)
            .await
            .unwrap();
        assert!(events.is_empty());

        // Insert events
        repository
            .insert_events(&[
                test_event(
                    &aggregate_id,
                    1,
                    CustomerEvent::NameAdded {
                        name: "Ferris".to_string(),
                    },
                ),
                test_event(
                    &aggregate_id,
                    2,
                    CustomerEvent::EmailUpdated {
                        new_email: "ferris@example.test".to_string(),
                    },
                ),
            ])
            .await
            .unwrap();
        let events = repository
            .get_events::<Customer>(&aggregate_id)
            .await
            .unwrap();
        assert_eq!(2, events.len());
        events
            .iter()
            .for_each(|e| assert_eq!(&aggregate_id, &e.aggregate_id));

        // Expect error when using invalid sequence
        let result = repository
            .insert_events(&[test_event(
                &aggregate_id,
                2,
                CustomerEvent::EmailUpdated {
                    new_email: "foo@example.test".to_string(),
                },
            )])
            .await
            .unwrap_err();

        match result {
            MongoAggregateError => {}
            _ => panic!("Expected OptimisticLockError, got {:?}", result),
        }
    }

    #[tokio::test]
    async fn test_snapshot_repository() {
        let client = mongodb_client().await;
        let repository = MongoEventRepository::new(client).with_streaming_channel_size(1);
        let aggregate_id = uuid::Uuid::new_v4().to_string();

        let snapshot = repository
            .get_snapshot::<Customer>(&aggregate_id)
            .await
            .unwrap();
        assert_eq!(None, snapshot);

        repository
            .update_snapshot::<Customer>(
                serde_json::to_value(Customer {
                    customer_id: "foo".to_string(),
                    name: "bar".to_string(),
                    email: "foo@example.test".to_string(),
                })
                .unwrap(),
                aggregate_id.clone(),
                1,
                &vec![],
            )
            .await
            .unwrap();

        let snapshot = repository
            .get_snapshot::<Customer>(&aggregate_id)
            .await
            .unwrap();
        assert_eq!(
            Some(test_snapshot_context(
                aggregate_id,
                serde_json::to_value(Customer {
                    customer_id: "foo".to_string(),
                    name: "bar".to_string(),
                    email: "".to_string(),
                })
                .unwrap(),
                0,
                1
            )),
            snapshot
        );
    }
}
