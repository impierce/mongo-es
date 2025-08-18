use cqrs_es::{persist::PersistedEventStore, Aggregate, CqrsFramework, Query};

use crate::{MongoCqrs, MongoEventRepository};

pub async fn default_mongo_client(connection_string: &str) -> mongodb::Client {
    mongodb::Client::with_uri_str(connection_string)
        .await
        .expect("Failed to create MongoDB client")
}

pub async fn mongo_cqrs<A>(
    client: mongodb::Client,
    query_processor: Vec<Box<dyn Query<A>>>,
    services: A::Services,
) -> MongoCqrs<A>
where
    A: Aggregate,
{
    let repository = MongoEventRepository::new(client).await.unwrap();
    let store = PersistedEventStore::new_event_store(repository);
    CqrsFramework::new(store, query_processor, services)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::cqrs::mongo_cqrs;
    use crate::utils::tests::{mongodb_client, CustomerView};
    use crate::MongoViewRepository;
    use cqrs_es::doc::{Customer, CustomerService};
    use cqrs_es::persist::GenericQuery;

    type TestQueryRepository =
        GenericQuery<MongoViewRepository<CustomerView, Customer>, CustomerView, Customer>;

    #[tokio::test]
    async fn test_cqrs_framework() {
        let client = mongodb_client().await;
        let view_repository =
            MongoViewRepository::<CustomerView, Customer>::new("test_view", client.clone());
        let query = TestQueryRepository::new(Arc::new(view_repository));
        let _cqrs = mongo_cqrs(client, vec![Box::new(query)], CustomerService);
    }
}
