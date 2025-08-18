use cqrs_es::doc::{Customer, CustomerCommand, CustomerService};
use cqrs_es::persist::PersistedEventStore;
use cqrs_es::CqrsFramework;
use mongo_es::MongoEventRepository;

use testcontainers_modules::{mongo, testcontainers::runners::AsyncRunner};

const LOCAL_CONNECTION_STRING: &str = "mongodb://localhost:27017";

#[tokio::test]
async fn test_with_mongodb() {
    let container = mongo::Mongo::default().start().await.unwrap();
    let host_ip = container.get_host().await.unwrap();
    let host_port = container.get_host_port_ipv4(27017).await.unwrap();

    let connection_string = &format!("mongodb://{}:{}/test", host_ip, host_port);

    let client = mongodb::Client::with_uri_str(connection_string)
        .await
        .expect("Failed to create MongoDB client");

    let repository = MongoEventRepository::new(client);

    let store = PersistedEventStore::<MongoEventRepository, Customer>::new_event_store(repository);

    let cqrs = CqrsFramework::new(store, vec![], CustomerService::default());

    const AGGREGATE_ID: &str = "1";

    cqrs.execute(
        AGGREGATE_ID,
        CustomerCommand::AddCustomerName {
            name: "Ferris".to_string(),
        },
    )
    .await
    .unwrap();
}
