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

const DEFAULT_STREAMING_CHANNEL_SIZE: usize = 200;

/// An event repository using MongoDB for persistence.
pub struct MongoEventRepository {
    client: Client,
    event_collection: String,
    snapshot_collection: String,
    stream_channel_size: usize,
}

impl MongoEventRepository {
    /// Creates a new `MongoEventRepository` with the default collection names and default streaming channel size.
    pub async fn new(client: Client) -> mongodb::error::Result<Self> {
        let repository = Self {
            client,
            event_collection: DEFAULT_EVENT_COLLECTION.to_string(),
            snapshot_collection: DEFAULT_SNAPSHOT_COLLECTION.to_string(),
            stream_channel_size: DEFAULT_STREAMING_CHANNEL_SIZE,
        };
        // TODO: is this the right place to create indexes?
        repository.create_indexes().await?;
        Ok(repository)
    }

    /// Configures a `MongoEventRepository` to use custom collection names.
    ///
    /// Defaults:
    /// - `event_collection`: "events"
    /// - `snapshot_collection`: "snapshots"
    pub fn with_collection_names(self, event_collection: &str, snapshot_collection: &str) -> Self {
        Self {
            client: self.client,
            event_collection: event_collection.to_string(),
            snapshot_collection: snapshot_collection.to_string(),
            stream_channel_size: self.stream_channel_size,
        }
    }

    /// Configures a `MongoEventRepository` to use a custom streaming channel size.
    ///
    /// Default: `200`
    pub fn with_streaming_channel_size(self, stream_channel_size: usize) -> Self {
        Self {
            client: self.client,
            event_collection: self.event_collection,
            snapshot_collection: self.snapshot_collection,
            stream_channel_size,
        }
    }

    // helper: get collection by name from default database
    fn get_collection(&self, name: &str) -> Collection<Document> {
        self.client
            .default_database()
            .expect("Default database not configured")
            .collection::<Document>(name)
    }

    /// Creates indexes for the event and snapshot collections to ensure uniqueness.
    async fn create_indexes(&self) -> mongodb::error::Result<()> {
        let event_collection = self.get_collection(&self.event_collection);

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

        log::debug!(
            "Created compound index on `{}` collection",
            self.event_collection
        );

        let snapshot_collection = self.get_collection(&self.snapshot_collection);

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

        log::debug!(
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

        let collection: Collection<Document> = self.get_collection(&self.event_collection);

        let (documents, _) = Self::build_event_upsert_documents(events);

        // Running transactions in a session ensures atomicity.
        let mut session = self.client.start_session().await?;
        session.start_transaction().await?;
        let res = collection
            .insert_many(documents)
            .session(&mut session)
            .await?;
        session.commit_transaction().await?;

        log::debug!(
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

        let collection: Collection<Document> = self.get_collection(&self.snapshot_collection);

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

        log::debug!(
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
        let options = FindOptions::builder().sort(sort).build();
        let cursor = self
            .get_collection(collection)
            .find(filter)
            .with_options(options)
            .await?;
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
            log::debug!(
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
        let query = self
            .query_collection(
                &A::aggregate_type(),
                aggregate_id,
                &self.event_collection,
                0,
                None,
            )
            .await?;
        Ok(stream_events(query, self.stream_channel_size))
    }

    // https://github.com/serverlesstechnology/postgres-es/blob/main/src/event_repository.rs#L99C5-L100C96
    // TODO: aggregate id is unused here, `stream_events` function needs to be broken up
    async fn stream_all_events<A: Aggregate>(&self) -> Result<ReplayStream, PersistenceError> {
        let filter = doc! { "aggregate_type": A::aggregate_type() };
        let options = FindOptions::builder().sort(doc! { "sequence": 1 }).build();
        let cursor = self
            .get_collection(&self.event_collection)
            .find(filter)
            .with_options(options)
            .await
            .unwrap();
        Ok(stream_events(cursor, self.stream_channel_size))
    }
}

fn stream_events(mut cursor: Cursor<Document>, channel_size: usize) -> ReplayStream {
    let (mut feed, stream) = ReplayStream::new(channel_size);
    tokio::spawn(async move {
        while let Some(result) = cursor.next().await {
            match result {
                Ok(document) => {
                    if let Ok(event) = serialized_event(&document) {
                        if feed.push(Ok(event)).await.is_err() {
                            log::warn!("Could not push event to feed. Stopping event stream.");
                            return;
                        };
                    } else {
                        log::error!("Failed to deserialize event: {:?}", document);
                    }
                }
                Err(e) => {
                    log::error!("Error while streaming events: {}", e);
                    let e: MongoAggregateError = e.into();
                    if feed.push(Err(e.into())).await.is_err() {
                        log::warn!("Could not push error to feed. Stopping event stream.");
                        return;
                    }
                }
            }
        }
    });
    stream
}

#[cfg(test)]
mod tests {
    use super::*;

    use cqrs_es::doc::{Customer, CustomerEvent};

    use crate::utils::tests::{mongodb_client, test_event, test_snapshot_context};

    #[tokio::test]
    async fn test_event_repository_inserts_successfully() {
        let client = mongodb_client().await;
        let repository = MongoEventRepository::new(client)
            .await
            .unwrap()
            .with_streaming_channel_size(1);
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
    async fn test_event_repository_get_last_events() {
        let client = mongodb_client().await;
        let repository = MongoEventRepository::new(client)
            .await
            .unwrap()
            .with_streaming_channel_size(1);
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
                    3,
                    CustomerEvent::EmailUpdated {
                        new_email: "second@example.test".to_string(),
                    },
                ),
                test_event(
                    &aggregate_id,
                    2,
                    CustomerEvent::EmailUpdated {
                        new_email: "first@example.test".to_string(),
                    },
                ),
                test_event(
                    &aggregate_id,
                    4,
                    CustomerEvent::NameAdded {
                        name: "F3rr1s".to_string(),
                    },
                ),
            ])
            .await
            .unwrap();

        let events = repository
            .get_last_events::<Customer>(&aggregate_id, 3)
            .await
            .unwrap();
        assert_eq!(2, events.len());
        assert_eq!(3, events[0].sequence);
        assert_eq!(4, events[1].sequence);
    }

    #[tokio::test]
    async fn test_event_repository_invalid_sequence_throws_error() {
        let client = mongodb_client().await;
        let repository = MongoEventRepository::new(client)
            .await
            .unwrap()
            .with_streaming_channel_size(1);
        let aggregate_id = uuid::Uuid::new_v4().to_string();

        // Insert event
        repository
            .insert_events(&[test_event(
                &aggregate_id,
                1,
                CustomerEvent::EmailUpdated {
                    new_email: "first@example.test".to_string(),
                },
            )])
            .await
            .unwrap();

        // Transaction should fail if any of the events has invalid sequence number
        let result = repository
            .insert_events(&[
                test_event(
                    &aggregate_id,
                    2,
                    CustomerEvent::EmailUpdated {
                        new_email: "second@example.test".to_string(),
                    },
                ),
                test_event(
                    &aggregate_id,
                    1,
                    CustomerEvent::NameAdded {
                        name: "Ferris".to_string(),
                    },
                ),
            ])
            .await
            .unwrap_err();

        match result {
            MongoAggregateError::OptimisticLock => {}
            _ => panic!("Expected OptimisticLockError, got {result:?}"),
        }

        let events = repository
            .get_events::<Customer>(&aggregate_id)
            .await
            .unwrap();
        assert_eq!(1, events.len());
    }

    #[tokio::test]
    async fn test_event_repository_replay_stream_aggregate_instance() {
        let client = mongodb_client().await;
        let repository = MongoEventRepository::new(client)
            .await
            .unwrap()
            .with_streaming_channel_size(3);
        let aggregate_id = uuid::Uuid::new_v4().to_string();

        // Create 10 test events
        let events: Vec<SerializedEvent> = (1..=10)
            .map(|i| {
                test_event(
                    &aggregate_id,
                    i,
                    CustomerEvent::EmailUpdated {
                        new_email: format!("{i}@example.test").to_string(),
                    },
                )
            })
            .collect();
        repository.insert_events(&events).await.unwrap();

        let mut stream = repository
            .stream_events::<Customer>(&aggregate_id)
            .await
            .unwrap();
        let mut num_events = 0;
        while (stream.next::<Customer>(&None).await).is_some() {
            num_events += 1;
        }
        assert_eq!(10, num_events);
    }

    #[tokio::test]
    async fn test_event_repository_replay_stream_all_aggregate_type() {
        let client = mongodb_client().await;
        let repository = MongoEventRepository::new(client)
            .await
            .unwrap()
            .with_streaming_channel_size(3);
        let aggregate_ids: Vec<String> = (0..3).map(|_| uuid::Uuid::new_v4().to_string()).collect();

        clear_collection(repository.get_collection(&repository.event_collection)).await;

        // Create 10 test events for each aggregate instance
        for aggregate_id in &aggregate_ids {
            let events: Vec<SerializedEvent> = (1..=10)
                .map(|i| {
                    test_event(
                        &aggregate_id,
                        i,
                        CustomerEvent::EmailUpdated {
                            new_email: format!("{i}@example.test").to_string(),
                        },
                    )
                })
                .collect();
            repository.insert_events(&events).await.unwrap();
        }

        let mut stream = repository.stream_all_events::<Customer>().await.unwrap();
        let mut num_events = 0;
        while (stream.next::<Customer>(&None).await).is_some() {
            num_events += 1;
        }
        assert_eq!(30, num_events);
    }

    #[tokio::test]
    async fn test_snapshot_repository_empty_returns_none() {
        let client = mongodb_client().await;
        let repository = MongoEventRepository::new(client)
            .await
            .unwrap()
            .with_streaming_channel_size(1);
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
        let repository = MongoEventRepository::new(client)
            .await
            .unwrap()
            .with_streaming_channel_size(1);
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
                &[],
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
        let repository = MongoEventRepository::new(client)
            .await
            .unwrap()
            .with_streaming_channel_size(1);
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
                &[],
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
                &[],
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
        let repository = MongoEventRepository::new(client)
            .await
            .unwrap()
            .with_streaming_channel_size(1);
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
                &[],
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
                &[],
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

    /// Deletes all documents in the specified collection.
    async fn clear_collection(collection: Collection<Document>) {
        collection.delete_many(doc! {}).await.unwrap();
    }
}
