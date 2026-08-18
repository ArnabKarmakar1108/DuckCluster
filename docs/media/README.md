# Media assets

Static assets for the README and CLI docs (screenshots, demo video).

## Automated snapshots (metadata + PNGs)

The coordinator has **no durable metadata store** in-process; monitor state lives in memory
until workers re-register. For documentation and before/after comparisons, use:

```bash
# Cluster must be running (./scripts/start-cluster.sh or docker compose up)
pip install -r scripts/requirements-media.txt
playwright install chromium

python3 scripts/capture-media-snapshot.py --demo-query
```

Each capture writes:

```
docs/media/snapshots/
  manifest.json              # index of all captures
  latest/ -> <tag>/          # symlink to most recent run
  <tag>/
    metadata.json            # health, workers, monitor summary/shards JSON
    dashboard-full.png       # full dashboard page
    topology-viewport.png    # topology panel crop
```

`metadata.json` embeds API responses under `endpoints` and relative screenshot paths under
`screenshots`.

Optional flags:

| Flag | Purpose |
|------|---------|
| `--url` | Coordinator base URL (default `http://127.0.0.1:8080`) |
| `--tag` | Folder name (default UTC timestamp) |
| `--promote` | Copy shots into `green-topology.png` / `queries.png` for README |

## Hand-curated files

| File | Use |
|------|-----|
| `DuckCluster-demo.mp4` | README demo video |
| `green-topology.png`, `queries.png`, `shard-catalog.png` | Dashboard screenshots |
| `stale-topology.png`, `unhealthy-topology.png` | Degraded-state examples |
| `cli.png` | CLI doc screenshot |

Regenerate curated dashboard PNGs with `--promote` after verifying the capture, or copy
manually from `snapshots/latest/`.
