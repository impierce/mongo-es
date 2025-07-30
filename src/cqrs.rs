use cqrs_es::{persist::PersistedEventStore, Aggregate, CqrsFramework, Query};

use crate::{MongoCqrs, MongoEventRepository};

pub async fn default_mongo_client(connection_string: &str) -> mongodb::Client {
    mongodb::Client::with_uri_str(connection_string)
        .await
        .expect("Failed to create MongoDB client")
}

pub fn mongo_cqrs<A>(
    client: mongodb::Client,
    query_processor: Vec<Box<dyn Query<A>>>,
    services: A::Services,
) -> MongoCqrs<A>
where
    A: Aggregate,
{
    let repository = MongoEventRepository::new(client);
    let store = PersistedEventStore::new_event_store(repository);
    CqrsFramework::new(store, query_processor, services)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::cqrs::mongo_cqrs;
    use crate::test_utils::tests::mongodb_client;
    use crate::MongoViewRepository;

    #[tokio::test]
    async fn test_cqrs_framework() {
        let client = mongodb_client().await;
        // let view_repository = MongoViewRepository::new("test_query", client.clone());
    }
}
