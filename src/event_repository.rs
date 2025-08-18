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
    bson::{self, doc, Document},
    options::IndexOptions,
    Cursor, IndexModel,
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

    // helper: get collection by name from default database
    fn collection(&self, name: &str) -> Collection<Document> {
        self.client
            .default_database()
            .expect("Default database not configured")
            .collection::<Document>(name)
    }

    async fn create_indexes(&self) -> mongodb::error::Result<()> {
        let event_collection = self.collection(&self.event_collection);

        let index = IndexModel::builder()
            .keys(doc! { "aggregate_id": 1, "sequence": 1 })
            .options(
                IndexOptions::builder()
                    .unique(true)
                    // .name("aggregate_id_sequence_unique".to_string())
                    .build(),
            )
            .build();

        event_collection.create_index(index).await?;

        println!(
            "Created compound index on `{}` collection",
            self.event_collection
        );

        let snapshot_collection = self.collection(&self.snapshot_collection);

        let index = IndexModel::builder()
            .keys(doc! { "aggregate_id": 1, "current_snapshot": -1 })
            .options(
                IndexOptions::builder()
                    .unique(true)
                    // .name("aggregate_id_current_snapshot_unique".to_string())
                    .build(),
            )
            .build();

        snapshot_collection.create_index(index).await?;

        println!(
            "Created compound index on `{}` collection",
            self.snapshot_collection
        );

        Ok(())
    }

    pub(crate) async fn insert_events(
        &self,
        events: &[SerializedEvent],
    ) -> Result<(), MongoAggregateError> {
        if events.is_empty() {
            return Ok(());
        }

        let collection: Collection<Document> = self.collection(&self.event_collection);

        let (documents, _) = Self::build_event_upsert_documents(events);

        let res = collection.insert_many(documents).await?;

        println!(
            "Inserted {} events into `{}` collection",
            res.inserted_ids.len(),
            self.event_collection
        );

        Ok(())
    }

    // helper: transforms serialized events into MongoDB documents
    fn build_event_upsert_documents(events: &[SerializedEvent]) -> (Vec<Document>, usize) {
        let mut current_sequence: usize = 0;
        let mut documents: Vec<Document> = Vec::default();
        for event in events {
            current_sequence = event.sequence;
            documents.push(doc! {
                "aggregate_id": event.aggregate_id.clone(),
                "aggregate_type": event.aggregate_type.clone(),
                "sequence": event.sequence as i64,
                "event_type": event.event_type.clone(),
                "event_version": event.event_version.clone(),
                "payload": bson::to_bson(&event.payload).unwrap(),
                "metadata": bson::to_bson(&event.metadata).unwrap(),
            });
        }
        (documents, current_sequence)
    }

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
                None,
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
        let (_, current_sequence) = Self::build_event_upsert_documents(events);
        self.insert_events(events).await?;

        let collection: Collection<Document> = self.collection(&self.snapshot_collection);

        let expected_snapshot = current_snapshot - 1;

        let snapshot = doc! {
            "aggregate_type": A::aggregate_type(),
            "aggregate_id": &aggregate_id,
            "payload": bson::to_bson(&aggregate_payload).unwrap(),
            "current_sequence": current_sequence as i64,
            "current_snapshot": current_snapshot as i64,
        };

        let filter = doc! {
            "aggregate_id": &aggregate_id,
            "aggregate_type": A::aggregate_type(),
            "current_snapshot": expected_snapshot as i64,
        };

        // Replaces the snapshot entirely if it exists instead of patching it.
        let res = collection
            .replace_one(filter, snapshot)
            .upsert(true)
            .await
            .map_err(MongoAggregateError::from)?;

        if res.matched_count == 0 && res.upserted_id.is_none() {
            return Err(MongoAggregateError::OptimisticLock);
        }

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
        sort: Option<Document>,
    ) -> Result<Cursor<Document>, MongoAggregateError> {
        let filter = self.build_filter(aggregate_type, aggregate_id, min_sequence);
        let collection = self.collection(collection);

        let options = FindOptions::builder().sort(sort).build();

        let cursor = collection.find(filter).with_options(options).await?;
        Ok(cursor)
    }

    fn build_filter(
        &self,
        aggregate_type: &str,
        aggregate_id: &str,
        min_sequence: i64,
    ) -> Document {
        if min_sequence == 0 {
            doc! {
                "aggregate_type": aggregate_type,
                "aggregate_id": aggregate_id,
            }
        } else {
            doc! {
                "aggregate_type": aggregate_type,
                "aggregate_id": aggregate_id,
                "sequence": { "$gte": min_sequence },
            }
        }
    }
}

fn serialized_event(document: &Document) -> Result<SerializedEvent, MongoAggregateError> {
    let aggregate_id = document.get_str("aggregate_id")?.to_string();
    let sequence = document.get_i64("sequence")? as usize;
    let aggregate_type = document.get_str("aggregate_type")?.to_string();
    let event_type = document.get_str("event_type")?.to_string();
    let event_version = document.get_str("event_version")?.to_string();
    let payload = bson::from_bson(document.get("payload").unwrap().clone()).unwrap();
    let metadata = bson::from_bson(document.get("metadata").unwrap().clone()).unwrap();

    Ok(SerializedEvent {
        aggregate_id,
        sequence,
        aggregate_type,
        event_type,
        event_version,
        payload,
        metadata,
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
                Some(doc! { "current_snapshot": -1 }), // sort descending
            )
            .await?;

        if let Some(result) = cursor.next().await {
            let document = result.map_err(MongoAggregateError::from)?;
            let payload = bson::from_bson(document.get("payload").unwrap().clone()).unwrap();
            println!(
                "Found snapshot for `{}` with id `{}`",
                A::aggregate_type(),
                aggregate_id
            );
            Ok(Some(SerializedSnapshot {
                aggregate_id: aggregate_id.to_string(),
                aggregate: payload,
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
        Err(PersistenceError::UnknownError("Not yet implemented".into()))
    }

    // https://github.com/serverlesstechnology/postgres-es/blob/main/src/event_repository.rs#L99C5-L100C96
    // TODO: aggregate id is unused here, `stream_events` function needs to be broken up
    async fn stream_all_events<A: Aggregate>(&self) -> Result<ReplayStream, PersistenceError> {
        Err(PersistenceError::UnknownError("Not yet implemented".into()))
    }
}

#[cfg(test)]
mod tests {
    use cqrs_es::doc::{Customer, CustomerEvent};
    use cqrs_es::persist::PersistedEventRepository;

    use crate::error::MongoAggregateError;
    use crate::test_utils::tests::{mongodb_client, test_event, test_snapshot_context};
    use crate::MongoEventRepository;

    #[tokio::test]
    async fn test_event_repository_inserts_successfully() {
        let client = mongodb_client().await;
        let repository = MongoEventRepository::new(client).with_streaming_channel_size(1);
        repository
            .create_indexes()
            .await
            .expect("Failed to create indexes");
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
    }

    #[tokio::test]
    async fn test_event_repository_invalid_sequence_throws_error() {
        let client = mongodb_client().await;
        let repository = MongoEventRepository::new(client).with_streaming_channel_size(1);
        repository
            .create_indexes()
            .await
            .expect("Failed to create indexes");
        let aggregate_id = uuid::Uuid::new_v4().to_string();

        // Insert event
        repository
            .insert_events(&[test_event(
                &aggregate_id,
                1,
                CustomerEvent::NameAdded {
                    name: "Ferris".to_string(),
                },
            )])
            .await
            .unwrap();

        // Expect error when using invalid sequence
        let result = repository
            .insert_events(&[test_event(
                &aggregate_id,
                1,
                CustomerEvent::EmailUpdated {
                    new_email: "email@example.test".to_string(),
                },
            )])
            .await
            .unwrap_err();

        match result {
            MongoAggregateError::OptimisticLock => {}
            _ => panic!("Expected OptimisticLockError, got {result:?}"),
        }
    }

    #[tokio::test]
    async fn test_snapshot_repository_empty_returns_none() {
        let client = mongodb_client().await;
        let repository = MongoEventRepository::new(client).with_streaming_channel_size(1);
        let aggregate_id = uuid::Uuid::new_v4().to_string();

        let snapshot = repository
            .get_snapshot::<Customer>(&aggregate_id)
            .await
            .unwrap();
        assert_eq!(None, snapshot);
    }

    #[tokio::test]
    async fn test_snapshot_repository_inserts_successfully() {
        let client = mongodb_client().await;
        let repository = MongoEventRepository::new(client).with_streaming_channel_size(1);
        let aggregate_id = uuid::Uuid::new_v4().to_string();

        repository
            .update_snapshot::<Customer>(
                serde_json::to_value(Customer {
                    customer_id: "123".to_string(),
                    name: "Ferris".to_string(),
                    email: "email@example.test".to_string(),
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
                aggregate_id.clone(),
                serde_json::to_value(Customer {
                    customer_id: "123".to_string(),
                    name: "Ferris".to_string(),
                    email: "email@example.test".to_string(),
                })
                .unwrap(),
                0,
                1
            )),
            snapshot
        );
    }

    #[tokio::test]
    async fn test_snapshot_repository_returns_latest_snapshot() {
        let client = mongodb_client().await;
        let repository = MongoEventRepository::new(client).with_streaming_channel_size(1);
        let aggregate_id = uuid::Uuid::new_v4().to_string();

        // First snapshot
        repository
            .update_snapshot::<Customer>(
                serde_json::to_value(Customer {
                    customer_id: "123".to_string(),
                    name: "Ferris".to_string(),
                    email: "first@example.test".to_string(),
                })
                .unwrap(),
                aggregate_id.clone(),
                1,
                &vec![],
            )
            .await
            .unwrap();

        // Second snapshot
        repository
            .update_snapshot::<Customer>(
                serde_json::to_value(Customer {
                    customer_id: "123".to_string(),
                    name: "Ferris".to_string(),
                    email: "second@example.test".to_string(),
                })
                .unwrap(),
                aggregate_id.clone(),
                2,
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
                aggregate_id.clone(),
                serde_json::to_value(Customer {
                    customer_id: "123".to_string(),
                    name: "Ferris".to_string(),
                    email: "second@example.test".to_string()
                })
                .unwrap(),
                0,
                2
            )),
            snapshot
        );
    }

    #[tokio::test]
    async fn test_snapshot_repository_invalid_sequence_returns_error() {
        let client = mongodb_client().await;
        let repository = MongoEventRepository::new(client).with_streaming_channel_size(1);
        let aggregate_id = uuid::Uuid::new_v4().to_string();

        // First snapshot
        repository
            .update_snapshot::<Customer>(
                serde_json::to_value(Customer {
                    customer_id: "123".to_string(),
                    name: "Ferris".to_string(),
                    email: "first@example.test".to_string(),
                })
                .unwrap(),
                aggregate_id.clone(),
                1,
                &vec![],
            )
            .await
            .unwrap();

        // Second snapshot, but with same sequence number
        let result = repository
            .update_snapshot::<Customer>(
                serde_json::to_value(Customer {
                    customer_id: "123".to_string(),
                    name: "Ferris".to_string(),
                    email: "second@example.test".to_string(),
                })
                .unwrap(),
                aggregate_id.clone(),
                1,
                &vec![],
            )
            .await
            .unwrap_err();

        match result {
            MongoAggregateError::OptimisticLock => {}
            _ => panic!("Expected OptimisticLockError, got {result:?}"),
        }

        let snapshot = repository
            .get_snapshot::<Customer>(&aggregate_id)
            .await
            .unwrap();

        assert_eq!(
            Some(test_snapshot_context(
                aggregate_id.clone(),
                serde_json::to_value(Customer {
                    customer_id: "123".to_string(),
                    name: "Ferris".to_string(),
                    email: "first@example.test".to_string()
                })
                .unwrap(),
                0,
                1
            )),
            snapshot
        );
    }
}
