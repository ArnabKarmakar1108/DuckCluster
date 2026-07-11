# Plan: DuckCluster CLI

**Status:** Shipped (MVP). See [`cli/README.md`](../cli/README.md) for usage and screenshot.

## Delivered

- `shell` (default on TTY), `query`, `status`, `workers`
- Table + JSON output, pre-flight validation, readline history
- Launcher: `./scripts/duckcluster` or `pip install -e cli/`

## TODO

- [ ] Shell completion (bash/zsh)
- [ ] `duckcluster query --explain` (needs plan-only REST endpoint)
- [ ] Async / long-running query polling (needs async API)

## Non-goals

Cluster start/stop, shard upload, datagen, auth — stay in `scripts/` or future work.
