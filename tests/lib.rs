use std::collections::HashMap;

use cqrs_es::doc::{Customer, CustomerEvent};
use cqrs_es::persist::PersistedEventStore;
use cqrs_es::EventStore;
use mongo_es::{default_mongo_client, MongoEventRepository};

use testcontainers_modules::testcontainers::{
    core::{ExecCommand, IntoContainerPort, WaitFor},
    runners::AsyncRunner,
    ContainerAsync, GenericImage, ImageExt,
};

#[tokio::test]
async fn test_event_store() {
    let (_container, connection_string) = mongodb().await;
    let client = default_mongo_client(&connection_string).await;

    let repository = MongoEventRepository::new(client).await.unwrap();
    let event_store =
        PersistedEventStore::<MongoEventRepository, Customer>::new_event_store(repository);

    commit_and_load(&event_store).await;
}

#[tokio::test]
async fn test_aggregate_store() {
    let (_container, connection_string) = mongodb().await;
    let client = default_mongo_client(&connection_string).await;

    let repository = MongoEventRepository::new(client).await.unwrap();
    let aggregate_store =
        PersistedEventStore::<MongoEventRepository, Customer>::new_aggregate_store(repository);

    commit_and_load(&aggregate_store).await;
}

#[tokio::test]
async fn test_snapshot_store() {
    let (_container, connection_string) = mongodb().await;
    let client = default_mongo_client(&connection_string).await;

    let repository = MongoEventRepository::new(client).await.unwrap();
    let snapshot_store =
        PersistedEventStore::<MongoEventRepository, Customer>::new_snapshot_store(repository, 3);

    commit_and_load(&snapshot_store).await;
}

/// Starts MongoDB as a single-node replica set and returns its connection string.
///
/// `testcontainers_modules::mongo::Mongo::repl_set` waits for log messages that MongoDB 8 and newer no longer emit.
async fn mongodb() -> (ContainerAsync<GenericImage>, String) {
    let container = GenericImage::new("mongo", "9")
        .with_exposed_port(27017.tcp())
        .with_wait_for(WaitFor::message_on_stdout("Waiting for connections"))
        .with_cmd(["--replSet", "rs0"])
        .start()
        .await
        .unwrap();
    container
        .exec(
            ExecCommand::new(["mongosh", "--quiet", "--eval", "rs.initiate()"])
                .with_container_ready_conditions(vec![WaitFor::message_on_stdout(
                    "Transition to primary complete",
                )]),
        )
        .await
        .unwrap();

    // The replica set advertises its member under the container's internal hostname, so connect directly.
    let connection_string = format!(
        "mongodb://{}:{}/test?directConnection=true",
        container.get_host().await.unwrap(),
        container.get_host_port_ipv4(27017).await.unwrap()
    );

    (container, connection_string)
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
