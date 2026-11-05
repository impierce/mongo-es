# Release process

Releases are automated with [release-plz](https://release-plz.dev/) and follow
[Cargo's SemVer rules](https://doc.rust-lang.org/cargo/reference/semver.html). Versions are never
bumped by hand, and `CHANGELOG.md` is never edited by hand. Releases up to 0.4.2 were made by
hand, so they have no changelog entry, GitHub release or provenance.

## Flow

1. On every push to `main`, release-plz opens or updates a release pull request
   (`chore: release vX.Y.Z`, labelled `release`). It contains the next version in `Cargo.toml` and
   a new `CHANGELOG.md` entry, both derived from the
   [Conventional Commits](https://www.conventionalcommits.org/) on `main` since the last release.
   [`cargo-semver-checks`](https://github.com/obi1kenobi/cargo-semver-checks) makes sure an
   API-breaking change can't go out as a compatible version. CI already runs it on each pull
   request, so a breaking change is marked with `!` in its title before it's merged.
2. A maintainer reviews the version and changelog. To adjust them, commit to the release branch
   shortly before merging, since release-plz regenerates the pull request whenever `main` changes.
3. Approving and merging the release pull request publishes the crate to
   [crates.io](https://crates.io/crates/mongo-es), then tags the commit as `vX.Y.Z` and creates
   a GitHub release with the same changelog.
4. The workflow then attaches the published `.crate` to the GitHub release, as crates.io serves it,
   together with its signed [SLSA](https://slsa.dev/) build provenance
   (`mongo-es-X.Y.Z.crate.intoto.jsonl`); see [Verifying a release](#verifying-a-release).

## Verifying a release

Each release after 0.4.2 carries SLSA build level 3 provenance, made by
[`slsa-github-generator`](https://github.com/slsa-framework/slsa-github-generator) and signed through
Sigstore. It states that the `.crate` was published by `release-plz.yaml` in this repository, run by
the push to `main` that merged the release pull request; the commit it names is the one `vX.Y.Z`
points to. To check a crate, download both files from the GitHub release and run
[`slsa-verifier`](https://github.com/slsa-framework/slsa-verifier):

```sh
slsa-verifier verify-artifact mongo-es-X.Y.Z.crate \
  --provenance-path mongo-es-X.Y.Z.crate.intoto.jsonl \
  --source-uri github.com/impierce/mongo-es \
  --source-branch main
```

If attaching them fails after the crate is published (crates.io slow to index it, a GitHub
outage), run the Release-plz workflow by hand from the release's tag (Actions → Release-plz → Run
workflow → Use workflow from → Tags → `vX.Y.Z`). It attaches the crate and provenance to that
release without publishing anything, and refuses to run from a branch. Its provenance names the
tag and the commit it points to, so verify such a release with `--source-tag vX.Y.Z` in place of
`--source-branch main`.

The attached `.crate` is the file crates.io serves: the workflow checks it against the checksum in
the crates.io index before attaching it. That checksum is also the `checksum` of `mongo-es` in a
dependent's `Cargo.lock`, so `sha256sum mongo-es-X.Y.Z.crate` ties the provenance to the crate a
build actually used.

## Versioning

Before 1.0, a breaking change (`feat!:`, `fix!:` or one detected by `cargo-semver-checks`) bumps the
minor version (`0.4.2` → `0.5.0`), and anything else bumps the patch version (`0.4.2` → `0.4.3`).

Pull requests are squash-merged, so the pull request title is the commit that release-plz reads.
[`DEVELOPMENT.md`](DEVELOPMENT.md#pull-requests) describes the title format.

## Changelog

Only commits that matter to users of the crate appear in the changelog: `ci`, `test`, `style` and
`chore` commits are left out, except dependency updates (`chore(deps)`). The rules are configured in
[`release-plz.toml`](../release-plz.toml).

## Credentials

The [release workflow](../.github/workflows/release-plz.yaml) runs without long-lived publishing
credentials:

- crates.io accepts the upload through
  [trusted publishing](https://crates.io/docs/trusted-publishing), only from `release-plz.yaml`
  running in the `release` environment, which is limited to `main`.
- The release pull request, tags and GitHub releases are created by the `impierce-release-plz`
  GitHub App, so CI runs on the release pull request like on any other. The App's credentials are
  stored in the variable `RELEASE_PLZ_CLIENT_ID` and the secret
  `RELEASE_PLZ_PRIVATE_KEY`.
