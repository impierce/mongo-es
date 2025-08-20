use cqrs_es::{persist::PersistedEventStore, CqrsFramework};

use crate::MongoEventRepository;

pub use mongodb::Client;

pub type MongoCqrs<A> = CqrsFramework<A, PersistedEventStore<MongoEventRepository, A>>;
