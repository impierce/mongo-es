use cqrs_es::persist::PersistenceError;

#[derive(Debug)]
pub enum MongoAggregateError {
    OptimisticLock,
    ConnectionError(Box<dyn std::error::Error + Send + Sync + 'static>),
    DeserializationError(Box<dyn std::error::Error + Send + Sync + 'static>),
    UnknownError(Box<dyn std::error::Error + Send + Sync + 'static>),
}

// impl std::error::Error for MongoAggregateError {}

impl From<serde_json::Error> for MongoAggregateError {
    fn from(error: serde_json::Error) -> Self {
        MongoAggregateError::UnknownError(Box::new(error))
    }
}

impl From<mongodb::error::Error> for MongoAggregateError {
    fn from(error: mongodb::error::Error) -> Self {
        match *error.kind {
            mongodb::error::ErrorKind::BsonDeserialization(_) => {
                MongoAggregateError::DeserializationError(Box::new(error))
            }
            _ => MongoAggregateError::UnknownError(Box::new(error)),
        }
    }
}

impl From<mongodb::bson::document::ValueAccessError> for MongoAggregateError {
    fn from(error: mongodb::bson::document::ValueAccessError) -> Self {
        // TODO: is this a deserialization error?
        MongoAggregateError::UnknownError(Box::new(error))
    }
}

impl From<MongoAggregateError> for PersistenceError {
    fn from(error: MongoAggregateError) -> Self {
        match error {
            MongoAggregateError::OptimisticLock => PersistenceError::OptimisticLockError,
            MongoAggregateError::ConnectionError(err) => PersistenceError::ConnectionError(err),
            MongoAggregateError::DeserializationError(err) => {
                PersistenceError::DeserializationError(err)
            }
            MongoAggregateError::UnknownError(err) => PersistenceError::UnknownError(err),
        }
    }
}
