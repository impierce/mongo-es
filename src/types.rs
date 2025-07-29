use cqrs_es::{persist::PersistedEventStore, CqrsFramework};

use crate::MongoEventRepository;

pub type MongoCqrs<A> = CqrsFramework<A, PersistedEventStore<MongoEventRepository, A>>;
