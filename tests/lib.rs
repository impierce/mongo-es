use std::collections::HashMap;

use cqrs_es::doc::{Customer, CustomerEvent};
use cqrs_es::persist::PersistedEventStore;
use cqrs_es::EventStore;
use mongo_es::{default_mongo_client, MongoEventRepository};

use testcontainers_modules::{mongo, testcontainers::runners::AsyncRunner};

#[tokio::test]
async fn test_event_store() {
    let container = mongo::Mongo::repl_set().start().await.unwrap();
    let host_ip = container.get_host().await.unwrap();
    let host_port = container.get_host_port_ipv4(27017).await.unwrap();

    let connection_string = format!("mongodb://{host_ip}:{host_port}/test?directConnection=true");
    let client = default_mongo_client(&connection_string).await;

    let repository = MongoEventRepository::new(client).await.unwrap();
    let event_store =
        PersistedEventStore::<MongoEventRepository, Customer>::new_event_store(repository);

    commit_and_load(&event_store).await;
}

#[tokio::test]
async fn test_aggregate_store() {
    let container = mongo::Mongo::repl_set().start().await.unwrap();
    let host_ip = container.get_host().await.unwrap();
    let host_port = container.get_host_port_ipv4(27017).await.unwrap();

    let connection_string = format!("mongodb://{host_ip}:{host_port}/test?directConnection=true");
    let client = default_mongo_client(&connection_string).await;

    let repository = MongoEventRepository::new(client).await.unwrap();
    let aggregate_store =
        PersistedEventStore::<MongoEventRepository, Customer>::new_aggregate_store(repository);

    commit_and_load(&aggregate_store).await;
}

#[tokio::test]
async fn test_snapshot_store() {
    let container = mongo::Mongo::repl_set().start().await.unwrap();
    let host_ip = container.get_host().await.unwrap();
    let host_port = container.get_host_port_ipv4(27017).await.unwrap();

    let connection_string = format!("mongodb://{host_ip}:{host_port}/test?directConnection=true");
    let client = default_mongo_client(&connection_string).await;

    let repository = MongoEventRepository::new(client).await.unwrap();
    let snapshot_store =
        PersistedEventStore::<MongoEventRepository, Customer>::new_snapshot_store(repository, 3);

    commit_and_load(&snapshot_store).await;
}

async fn commit_and_load(store: &PersistedEventStore<MongoEventRepository, Customer>) {
    const AGGREGATE_ID: &str = "1";

    assert_eq!(0, store.load_events(AGGREGATE_ID).await.unwrap().len());

    let context = store.load_aggregate(AGGREGATE_ID).await.unwrap();

    store
        .commit(
            vec![
                CustomerEvent::NameAdded {
                    name: "Ferris".to_string(),
                },
                CustomerEvent::EmailUpdated {
                    new_email: "ferris@example.test".to_string(),
                },
            ],
            context,
            HashMap::default(),
        )
        .await
        .unwrap();

    assert_eq!(2, store.load_events(AGGREGATE_ID).await.unwrap().len());
}
