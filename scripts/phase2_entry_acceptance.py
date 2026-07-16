"""Run and freeze the API-only ingestion portion of the phase-two entry gate."""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import urllib.error
import urllib.request
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast


def _readiness(base_url: str) -> dict[str, Any]:
    url = f"{base_url.rstrip('/')}/api/v1/health/ready"
    try:
        with urllib.request.urlopen(url, timeout=10) as response:  # noqa: S310
            payload = cast(dict[str, Any], json.load(response))
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")[:2000]
        raise RuntimeError(f"readiness failed with HTTP {exc.code}: {detail}") from exc
    if payload.get("ready") is not True:
        raise RuntimeError("application readiness is not green")
    required = {"mysql", "redis", "milvus", "neo4j", "model", "object_store"}
    dependencies = payload.get("dependencies", {})
    missing = sorted(required.difference(dependencies))
    if missing:
        raise RuntimeError(f"readiness is missing dependencies: {', '.join(missing)}")
    if any(dependencies[name].get("mode") == "fake" for name in required if name in dependencies):
        raise RuntimeError("phase-two entry acceptance forbids Fake dependency readiness")
    return payload


def _run(command: list[str], *, cwd: Path) -> str:
    result = subprocess.run(  # noqa: S603
        command,
        cwd=cwd,
        check=True,
        capture_output=True,
        text=True,
    )
    return result.stdout.strip()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("dataset", type=Path)
    parser.add_argument("--base-url", required=True)
    parser.add_argument("--token-file", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()

    root = Path(__file__).resolve().parents[1]
    dataset = args.dataset.resolve()
    token_file = args.token_file.resolve()
    output = args.output.resolve()
    if output.exists():
        raise SystemExit(f"refusing to overwrite frozen report: {output}")
    if not (dataset / "manifest.json").is_file():
        raise SystemExit(f"dataset manifest is missing: {dataset}")
    if not token_file.is_file() or not token_file.read_text(encoding="utf-8").strip():
        raise SystemExit("token file is missing or empty")

    readiness = _readiness(args.base_url)
    _run(
        [
            "uv",
            "run",
            "--project",
            "synthetic-data",
            "graphrag-data",
            "validate",
            str(dataset),
        ],
        cwd=root,
    )
    upload_stdout = _run(
        [
            "uv",
            "run",
            "--project",
            "synthetic-data",
            "graphrag-data",
            "upload",
            str(dataset),
            "--base-url",
            args.base_url,
            "--token-file",
            str(token_file),
        ],
        cwd=root,
    )
    upload_results = cast(list[dict[str, Any]], json.loads(upload_stdout))
    failed = [
        item
        for item in upload_results
        if not isinstance(item.get("task"), dict)
        or item["task"].get("status") != "completed"
    ]
    if failed:
        raise RuntimeError(f"{len(failed)} uploads did not reach completed")

    manifest_bytes = (dataset / "manifest.json").read_bytes()
    manifest = json.loads(manifest_bytes)
    commit = _run(["git", "rev-parse", "HEAD"], cwd=root)
    report = {
        "schema_version": "phase2-entry-ingestion-v1",
        "status": "api_ingestion_passed_rebuild_and_failure_checks_pending",
        "created_at": datetime.now(UTC).isoformat(),
        "git_commit": commit,
        "dataset_id": manifest.get("dataset_id"),
        "dataset_version": manifest.get("dataset_version"),
        "manifest_sha256": hashlib.sha256(manifest_bytes).hexdigest(),
        "readiness": readiness,
        "upload_count": len(upload_results),
        "upload_results": upload_results,
        "remaining_gate": [
            "anchor_retrieval_acl_citation",
            "milvus_neo4j_destroy_and_rebuild",
            "refusal_cross_tenant_dependency_failure_partial_write",
        ],
    }
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(
        json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(f"frozen API-ingestion report: {output}")


if __name__ == "__main__":
    main()
