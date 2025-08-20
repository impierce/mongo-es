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
mongo-es = "0.3.0"
```

## Development

All unit tests run against a local MongoDB instance which can be started using the provided `compose.yaml` file. A default "standalone" MongoDB instance does not support transactions, so a single-node replica set is configured.

> [!NOTE]
> Unit tests need to be run **serially** due to a single shared MongoDB instance for all tests. If tests are executed in parallel, assertions can happen concurrently and produce flaky results. Serial execution can be achieved by running the tests with [nextest](https://nexte.st) or by adding a simple option to `cargo test` as described below.

```shell
docker compose up -d

cargo nextest run
# or
cargo test -- --test-threads=1
```
