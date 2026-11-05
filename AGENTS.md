# AGENTS.md

## Agent skills

### Issue tracker

Issues are tracked in GitHub Issues on `impierce/mongo-es` via the `gh` CLI. See `docs/agents/issue-tracker.md`.

### Triage labels

The five default triage roles, each label named after its role (`needs-triage`, `needs-info`, `ready-for-agent`, `ready-for-human`, `wontfix`). See `docs/agents/triage-labels.md`.

### Domain docs

Single-context: one `GLOSSARY.md` and `docs/adr/` at the repo root, created only when first needed. See `docs/agents/domain.md`.

## Commits and pull requests

Pull requests are squash-merged; the title is the commit on `main` and must follow the Conventional Commits style in `docs/DEVELOPMENT.md#pull-requests` (terse, no articles, code literals in backticks). Use the same style for commit messages on branches.
