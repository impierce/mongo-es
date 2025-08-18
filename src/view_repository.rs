use async_trait::async_trait;
use cqrs_es::persist::{PersistenceError, ViewContext, ViewRepository};
use cqrs_es::{Aggregate, View};
use mongodb::bson::{self, doc, Document};

use crate::utils::load_view;

pub struct MongoViewRepository<V, A> {
    _phantom: std::marker::PhantomData<(V, A)>,
    view_name: String,
    client: mongodb::Client,
}

impl<V, A> MongoViewRepository<V, A>
where
    V: View<A>,
    A: Aggregate,
{
    pub fn new(view_name: &str, client: mongodb::Client) -> Self {
        Self {
            _phantom: Default::default(),
            view_name: view_name.to_string(),
            client,
        }
    }
}

#[async_trait]
impl<V, A> ViewRepository<V, A> for MongoViewRepository<V, A>
where
    V: View<A>,
    A: Aggregate,
{
    async fn load(&self, view_id: &str) -> Result<Option<V>, PersistenceError> {
        let result = load_view(&self.client, &self.view_name, view_id).await?;
        let document = match result {
            Some(item) => item,
            None => return Ok(None),
        };

        let payload = bson::from_bson(document.get("payload").unwrap().clone()).unwrap();
        let view: V = serde_json::from_value(payload)?;
        Ok(Some(view))
    }

    async fn load_with_context(
        &self,
        view_id: &str,
    ) -> Result<Option<(V, ViewContext)>, PersistenceError> {
        let result = load_view(&self.client, &self.view_name, view_id).await?;
        let document = match result {
            Some(item) => item,
            None => return Ok(None),
        };

        let version = document.get_i64("version").unwrap_or(0);
        let payload = bson::from_bson(document.get("payload").unwrap().clone()).unwrap();
        let view: V = serde_json::from_value(payload)?;
        let context = ViewContext::new(view_id.to_string(), version);
        Ok(Some((view, context)))
    }

    async fn update_view(&self, view: V, context: ViewContext) -> Result<(), PersistenceError> {
        let collection = self
            .client
            .default_database()
            .expect("Default database not configured")
            .collection::<Document>(&self.view_name);

        let view_id = context.view_instance_id;

        let filter = doc! { "view_id": &view_id };
        let update = doc! {
            "$set": {
                "payload": bson::to_bson(&view).unwrap(),
                "version": context.version + 1,
            }
        };

        let res = collection
            .update_one(filter, update)
            .upsert(true)
            .await
            .expect("Failed to update view");

        println!(
            "Modified {} documents in `{}` collection",
            res.modified_count, &self.view_name
        );

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use cqrs_es::doc::{Customer, CustomerEvent};
    use cqrs_es::persist::{ViewContext, ViewRepository};

    use crate::utils::tests::{mongodb_client, CustomerView};
    use crate::MongoViewRepository;

    #[tokio::test]
    async fn test_view_repository() {
        let repository =
            MongoViewRepository::<CustomerView, Customer>::new("test_view", mongodb_client().await);

        let test_view_id = uuid::Uuid::new_v4().to_string();

        let view = CustomerView {
            events: vec![CustomerEvent::NameAdded {
                name: "Ferris".to_string(),
            }],
        };

        repository
            .update_view(view.clone(), ViewContext::new(test_view_id.to_string(), 0))
            .await
            .unwrap();

        let found = repository.load(&test_view_id).await.unwrap().unwrap();

        assert_eq!(found, view);
    }
}
