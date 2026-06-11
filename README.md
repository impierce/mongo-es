# mongo-es

[![Crates.io Version](https://img.shields.io/crates/v/mongo-es)](https://crates.io/crates/mongo-es)
[![codecov](https://codecov.io/gh/impierce/mongo-es/graph/badge.svg?token=69OQPQVDM4)](https://codecov.io/gh/impierce/mongo-es)
![Check dependencies](https://github.com/impierce/mongo-es/actions/workflows/audit.yaml/badge.svg)

A MongoDB implementation of the `PersistedEventRepository` trait in [cqrs-es](https://crates.io/crates/cqrs-es).

---

## Usage

Add the following to your `Cargo.toml`:

```toml
[dependencies]
cqrs-es = "0.5.0"
mongo-es = "0.4.1"
```

It is recommended to follow the [official documentation](https://doc.rust-cqrs.org) to build your CQRS application.

### Useful Links

- [Development](docs/DEVELOPMENT.md)
