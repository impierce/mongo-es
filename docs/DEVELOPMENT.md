# Development

## Testing

All unit tests run against a local MongoDB instance which can be started using the provided `compose.yaml` file. A default "standalone" MongoDB instance does not support transactions, so a single-node replica set is configured.

> [!NOTE]
> Unit tests need to be run **serially** due to a single shared MongoDB instance for all tests. If tests are executed in parallel, assertions can happen concurrently and produce flaky results. Serial execution can be achieved by running the tests with [nextest](https://nexte.st) or by adding a simple option to `cargo test` as described below.

```shell
docker compose up -d

cargo nextest run
# or
cargo test -- --test-threads=1
```
