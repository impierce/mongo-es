use cqrs_es::doc::Customer;
use cqrs_es::persist::PersistedEventStore;
use mongo_es::MongoEventRepository;

use testcontainers_modules::{mongo, testcontainers::runners::AsyncRunner};

const TEST_CONNECTION_STRING: &str = "mongodb://localhost:27017";

// fn new_test_event_store() -> PersistedEventStore<MongoEventRepository, Customer> {
//     let repository = MongoEventRepository::new();
//     PersistedEventStore::<MongoEventRepository, Customer>::new_event_store(repository)
// }

#[tokio::test]
async fn test_with_mongodb() {
    let container = mongo::Mongo::default().start().await.unwrap();
    let host_ip = container.get_host().await.unwrap();
    let host_port = container.get_host_port_ipv4(27017).await.unwrap();

    let connection_string = &format!("mongodb://{}:{}", host_ip, host_port);
    assert_eq!(connection_string, "mongodb://localhost:27017");
}
