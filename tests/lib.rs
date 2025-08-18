use cqrs_es::doc::{Customer, CustomerCommand, CustomerService};
use cqrs_es::persist::PersistedEventStore;
use cqrs_es::CqrsFramework;
use mongo_es::{default_mongo_client, MongoEventRepository};

use testcontainers_modules::{mongo, testcontainers::runners::AsyncRunner};

#[tokio::test]
async fn test_with_mongodb_container() {
    let container = mongo::Mongo::default().start().await.unwrap();
    let host_ip = container.get_host().await.unwrap();
    let host_port = container.get_host_port_ipv4(27017).await.unwrap();

    let connection_string = &format!("mongodb://{host_ip}:{host_port}/test");

    let client = default_mongo_client(connection_string).await;

    let repository = MongoEventRepository::new(client).await.unwrap();

    let store = PersistedEventStore::<MongoEventRepository, Customer>::new_event_store(repository);

    let cqrs = CqrsFramework::new(store, vec![], CustomerService);

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
