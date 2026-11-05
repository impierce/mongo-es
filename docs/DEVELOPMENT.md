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

## Pull requests

Pull requests are squash-merged, so the **pull request title** becomes the commit message on `main`.
It must follow [Conventional Commits](https://www.conventionalcommits.org/), which CI checks:

- `fix: order events by sequence`
- `` feat: add `with_batch_size` option ``
- `` feat!: rename `MongoEventRepository::new` `` (breaking change)

The title also determines the next version number and the changelog entry, so describe the change from a user's point of view.

A change that breaks the public API needs `!` in the title (`feat!:`, `fix!:`): before 1.0 it bumps the minor version, and the changelog lists it as breaking.
CI's `SemVer` job runs [`cargo-semver-checks`](https://github.com/obi1kenobi/cargo-semver-checks) on the public API, with all features, against the pull request's base, unless the title already has the `!`, and fails when it finds a breaking change.
After adding the `!`, re-run that job: it reads the current title, but editing the title doesn't start it.

## Releases

Releases are automated with [release-plz](https://release-plz.dev/): it keeps a release pull request open with the next version and changelog, and publishes to crates.io once that pull request is merged.
Don't bump the version or edit `CHANGELOG.md` by hand.
[`RELEASE.md`](RELEASE.md) describes the full process.
