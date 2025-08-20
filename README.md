# mongo-es

[![Crates.io Version](https://img.shields.io/crates/v/mongo-es)](https://crates.io/crates/mongo-es)
[![codecov](https://codecov.io/gh/impierce/mongo-es/graph/badge.svg?token=69OQPQVDM4)](https://codecov.io/gh/impierce/mongo-es)

A MongoDB implementation of the `PersistedEventRepository` trait in [cqrs-es](https://crates.io/crates/cqrs-es).

---

## Usage

Add the following to your `Cargo.toml`:

```toml
[dependencies]
cqrs-es = "0.4.12"
mongo-es = "0.2.1"
```

## Development

All unit tests run against a local MongoDB instance which can be started using the provided `compose.yaml` file. A "standalone" MongoDB instance does not support transactions, so a single-node replica set is configured.

```
docker compose up -d
cargo test
```
