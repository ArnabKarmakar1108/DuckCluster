#!/usr/bin/env python3
"""Capture dashboard screenshots and coordinator monitor metadata to disk.

Lightweight persistence for docs and regression comparison: each run writes a
timestamped folder with metadata.json (API payloads + screenshot paths) and PNGs.

Requires a running cluster and Playwright Chromium:

  pip install -r scripts/requirements-media.txt
  playwright install chromium

Example:

  ./scripts/start-cluster.sh   # or docker compose up
  python3 scripts/capture-media-snapshot.py --demo-query
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests

ROOT = Path(__file__).resolve().parent.parent
DEFAULT_OUT = ROOT / "docs" / "media" / "snapshots"


def fetch_json(base_url: str, path: str, timeout: float) -> dict[str, Any]:
    response = requests.get(f"{base_url}{path}", timeout=timeout)
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, dict):
        raise TypeError(f"expected JSON object from {path}")
    return payload


def run_demo_query(base_url: str, timeout: float) -> None:
    sql = (
        "SELECT category, COUNT(*) AS cnt FROM events "
        "GROUP BY category ORDER BY category"
    )
    response = requests.post(
        f"{base_url}/v1/query",
        json={"sql": sql},
        timeout=timeout,
    )
    if response.status_code >= 400:
        raise RuntimeError(f"demo query failed ({response.status_code}): {response.text}")


def wait_for_dashboard(page, timeout_ms: int) -> None:
    page.wait_for_selector("#topology-svg", timeout=timeout_ms)
    page.wait_for_function(
        """() => {
          const status = document.getElementById('cluster-status');
          return status && status.textContent && status.textContent !== '—';
        }""",
        timeout=timeout_ms,
    )
    time.sleep(1.5)


def capture_screenshots(
    base_url: str,
    dest: Path,
    width: int,
    timeout_ms: int,
) -> dict[str, str]:
    from playwright.sync_api import sync_playwright

    screenshots: dict[str, str] = {}
    dashboard_url = f"{base_url}/dashboard/index.html"

    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=True)
        page = browser.new_page(viewport={"width": width, "height": 900})
        page.goto(dashboard_url, wait_until="networkidle", timeout=timeout_ms)
        wait_for_dashboard(page, timeout_ms)

        full_name = "dashboard-full.png"
        page.screenshot(path=str(dest / full_name), full_page=True)
        screenshots["dashboardFull"] = full_name

        topology_name = "topology-viewport.png"
        topology = page.locator(".topology-wrap")
        if topology.count() > 0:
            topology.screenshot(path=str(dest / topology_name))
            screenshots["topologyViewport"] = topology_name

        browser.close()

    return screenshots


def update_manifest(out_dir: Path, tag: str, captured_at: str) -> None:
    manifest_path = out_dir / "manifest.json"
    manifest: dict[str, Any] = {"captures": []}
    if manifest_path.is_file():
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest.setdefault("captures", []).append(
        {"tag": tag, "path": tag, "capturedAt": captured_at}
    )
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")


def link_latest(out_dir: Path, tag: str) -> None:
    latest = out_dir / "latest"
    if latest.is_symlink() or latest.exists():
        latest.unlink()
    latest.symlink_to(tag, target_is_directory=True)


def promote_to_readme_assets(dest: Path, promote_map: dict[str, Path]) -> None:
    for source_name, target in promote_map.items():
        source = dest / source_name
        if not source.is_file():
            continue
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(source.read_bytes())


def main() -> int:
    parser = argparse.ArgumentParser(description="Capture dashboard PNGs + monitor metadata")
    parser.add_argument("--url", default="http://127.0.0.1:8080", help="Coordinator base URL")
    parser.add_argument("--out", type=Path, default=DEFAULT_OUT, help="Snapshots root directory")
    parser.add_argument("--tag", default=None, help="Capture folder name (default: UTC timestamp)")
    parser.add_argument("--width", type=int, default=1280, help="Browser viewport width")
    parser.add_argument("--timeout", type=float, default=120.0, help="HTTP timeout (seconds)")
    parser.add_argument(
        "--demo-query",
        action="store_true",
        help="Run a sample GROUP BY before capture (populates recent queries)",
    )
    parser.add_argument(
        "--promote",
        action="store_true",
        help="Copy topology/dashboard PNGs into docs/media/ README assets",
    )
    args = parser.parse_args()

    base_url = args.url.rstrip("/")
    tag = args.tag or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    dest = args.out / tag
    if dest.exists():
        print(f"ERROR: capture directory already exists: {dest}", file=sys.stderr)
        return 1
    dest.mkdir(parents=True)

    try:
        if args.demo_query:
            print("Running demo query...")
            run_demo_query(base_url, args.timeout)

        print("Fetching monitor metadata...")
        captured_at = datetime.now(timezone.utc).isoformat()
        metadata: dict[str, Any] = {
            "capturedAt": captured_at,
            "coordinatorUrl": base_url,
            "endpoints": {
                "health": fetch_json(base_url, "/v1/cluster/health", args.timeout),
                "workers": fetch_json(base_url, "/v1/cluster/workers", args.timeout),
                "monitorSummary": fetch_json(base_url, "/v1/monitor/summary", args.timeout),
                "monitorShards": fetch_json(base_url, "/v1/monitor/shards", args.timeout),
            },
            "screenshots": {},
        }

        print("Capturing screenshots (Playwright)...")
        timeout_ms = int(args.timeout * 1000)
        metadata["screenshots"] = capture_screenshots(base_url, dest, args.width, timeout_ms)

        metadata_path = dest / "metadata.json"
        metadata_path.write_text(json.dumps(metadata, indent=2) + "\n", encoding="utf-8")
        print(f"Wrote {metadata_path}")

        update_manifest(args.out, tag, captured_at)
        link_latest(args.out, tag)

        if args.promote:
            media = ROOT / "docs" / "media"
            promote_to_readme_assets(
                dest,
                {
                    "topology-viewport.png": media / "green-topology.png",
                    "dashboard-full.png": media / "queries.png",
                },
            )
            print(f"Promoted PNGs under {media}")

        print(f"Capture complete: {dest}")
        print(f"Latest symlink: {args.out / 'latest'}")
        return 0
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
