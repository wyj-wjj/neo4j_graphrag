"""Command-line entry point for safe local generation and validation."""

from __future__ import annotations

import argparse
import asyncio
import json
from collections.abc import Sequence
from pathlib import Path

import uvicorn

from graphrag_data_factory.constants import DATASET_VERSION
from graphrag_data_factory.deterministic import load_profile, project_root
from graphrag_data_factory.exporter import DatasetExporter, manifest_digest
from graphrag_data_factory.factory import DatasetFactory
from graphrag_data_factory.release import (
    RELEASE_ASSETS,
    RELEASE_DOWNLOAD_BASE_URL,
    default_release_output_root,
    fetch_release_profiles,
)
from graphrag_data_factory.resources import (
    assert_large_output_path,
    estimate_generation,
    execute_cleanup,
    plan_cleanup,
)
from graphrag_data_factory.schema_export import export_schemas
from graphrag_data_factory.simulator import SimulatorSettings, create_simulator_app
from graphrag_data_factory.streaming_factory import StreamingProfileExporter
from graphrag_data_factory.streaming_validator import StreamingDatasetValidator
from graphrag_data_factory.upload_client import KnowledgeUploadClient


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="GraphRAG deterministic synthetic data factory")
    subparsers = parser.add_subparsers(dest="command", required=True)

    generate = subparsers.add_parser("generate", help="generate and atomically validate a dataset")
    generate.add_argument("--profile", required=True)
    generate.add_argument("--output", type=Path)
    generate.add_argument("--replace", action="store_true")
    generate.add_argument("--allow-large", action="store_true")
    generate.add_argument("--confirm-profile")
    generate.add_argument("--confirm-estimated-bytes", type=int)
    generate.add_argument(
        "--resume",
        action="store_true",
        help="resume an explicitly confirmed large generation from its fsynced checkpoint",
    )

    validate = subparsers.add_parser("validate", help="validate a published dataset")
    validate.add_argument("dataset", type=Path)

    schemas = subparsers.add_parser("schemas", help="refresh deterministic JSON Schema snapshots")
    schemas.add_argument("--output", type=Path, default=project_root() / "schemas")

    estimate = subparsers.add_parser("estimate", help="estimate records, disk and peak memory")
    estimate.add_argument("--profile", required=True)

    cleanup = subparsers.add_parser("cleanup", help="dry-run or execute manifest-owned cleanup")
    cleanup.add_argument("dataset", type=Path)
    cleanup.add_argument("--execute", action="store_true")
    cleanup.add_argument("--confirm-dataset-id")

    simulator = subparsers.add_parser(
        "serve-simulator", help="serve the non-production business simulator on loopback"
    )
    simulator.add_argument("dataset", type=Path)
    simulator.add_argument("--host", default="127.0.0.1")
    simulator.add_argument("--port", type=int, default=8099)
    simulator.add_argument("--enable-fault-control", action="store_true")
    simulator.add_argument("--control-secret-file", type=Path)

    upload = subparsers.add_parser("upload", help="exercise the application's formal upload API")
    upload.add_argument("dataset", type=Path)
    upload.add_argument("--base-url", required=True)
    upload.add_argument("--token-file", type=Path, required=True)
    upload.add_argument("--include-expected-rejections", action="store_true")
    upload.add_argument("--no-wait", action="store_true")

    fetch_release = subparsers.add_parser(
        "fetch-release", help="download and verify immutable large-profile release assets"
    )
    fetch_release.add_argument(
        "--profile",
        choices=("all", *sorted(RELEASE_ASSETS)),
        default="all",
    )
    fetch_release.add_argument("--output-root", type=Path)
    fetch_release.add_argument("--release-base-url", default=RELEASE_DOWNLOAD_BASE_URL)
    fetch_release.add_argument("--replace", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.command == "generate":
        profile, profile_path = load_profile(args.profile)
        if profile.requires_explicit_large_flag and not args.allow_large:
            raise SystemExit(
                f"profile {profile.name} requires --allow-large; no files were generated"
            )
        estimate = estimate_generation(profile)
        if profile.requires_explicit_large_flag and (
            args.confirm_profile != profile.name
            or args.confirm_estimated_bytes != estimate.estimated_bytes
        ):
            raise SystemExit(
                f"profile {profile.name} requires --confirm-profile {profile.name} and "
                f"--confirm-estimated-bytes {estimate.estimated_bytes}"
            )
        output = args.output or _default_output(profile.name)
        if not output.is_absolute():
            output = project_root() / output
        assert_large_output_path(profile, output, project_root())
        if profile.name == "staging-large":
            raise SystemExit(
                "staging-large materialization remains disabled until core records use a "
                "disk-backed working set"
            )
        if profile.requires_explicit_large_flag:
            manifest = StreamingProfileExporter().export(
                profile,
                profile_path=profile_path,
                target=output,
                replace=args.replace,
                resume=args.resume,
            )
        else:
            if args.resume:
                raise SystemExit("--resume is available only for explicit large profiles")
            bundle = DatasetFactory(profile).generate()
            manifest = DatasetExporter().export(
                bundle,
                profile_path=profile_path,
                target=output,
                replace=args.replace,
            )
        print(
            f"validated synthetic dataset: {manifest.dataset_id} "
            f"files={len(manifest.files)} digest={manifest_digest(manifest)}"
        )
        return 0
    if args.command == "validate":
        dataset = args.dataset
        if not dataset.is_absolute():
            dataset = project_root() / dataset
        manifest = StreamingDatasetValidator().validate(dataset)
        print(
            f"valid synthetic dataset: {manifest.dataset_id} "
            f"records={sum(manifest.record_counts.values())}"
        )
        return 0
    if args.command == "schemas":
        output = args.output
        if not output.is_absolute():
            output = project_root() / output
        paths = export_schemas(output)
        print(f"wrote {len(paths)} schema snapshots to {output}")
        return 0
    if args.command == "estimate":
        profile, _ = load_profile(args.profile)
        print(json.dumps(estimate_generation(profile).model_dump(), sort_keys=True))
        return 0
    if args.command == "cleanup":
        dataset = args.dataset
        if not dataset.is_absolute():
            dataset = project_root() / dataset
        if args.execute:
            if not args.confirm_dataset_id:
                raise SystemExit("cleanup --execute requires --confirm-dataset-id")
            plan = execute_cleanup(dataset, confirm_dataset_id=args.confirm_dataset_id)
        else:
            plan = plan_cleanup(dataset)
        print(json.dumps(plan.model_dump(), ensure_ascii=False, sort_keys=True))
        return 0
    if args.command == "serve-simulator":
        if args.host not in {"127.0.0.1", "localhost", "::1"}:
            raise SystemExit("the synthetic simulator may bind only to a loopback host")
        secret = None
        if args.control_secret_file is not None:
            secret = args.control_secret_file.read_text(encoding="utf-8").strip()
        settings = SimulatorSettings(
            environment="development",
            fault_control_enabled=args.enable_fault_control,
            control_secret=secret,
        )
        app = create_simulator_app(args.dataset, settings)
        uvicorn.run(app, host=args.host, port=args.port, access_log=False)
        return 0
    if args.command == "upload":
        token = args.token_file.read_text(encoding="utf-8").strip()

        async def upload_dataset() -> None:
            async with KnowledgeUploadClient(
                base_url=args.base_url,
                bearer_token=token,
            ) as client:
                results = await client.upload_dataset(
                    args.dataset,
                    include_expected_rejections=args.include_expected_rejections,
                    wait_for_terminal=not args.no_wait,
                )
            print(
                json.dumps(
                    [result.model_dump(mode="json") for result in results],
                    ensure_ascii=False,
                    sort_keys=True,
                )
            )

        asyncio.run(upload_dataset())
        return 0
    if args.command == "fetch-release":
        output_root = args.output_root or default_release_output_root(project_root())
        if not output_root.is_absolute():
            output_root = project_root() / output_root
        profiles = tuple(sorted(RELEASE_ASSETS)) if args.profile == "all" else (args.profile,)
        validator = StreamingDatasetValidator().validate
        installed = fetch_release_profiles(
            profiles,
            output_root=output_root,
            release_base_url=args.release_base_url,
            replace=args.replace,
            validator=validator,
        )
        for dataset in installed:
            print(f"installed validated synthetic dataset: {dataset}")
        return 0
    raise AssertionError("unreachable command")


def _default_output(profile: str) -> Path:
    if profile == "ci-small":
        return project_root() / "fixtures" / "ci-small"
    return project_root() / "generated" / DATASET_VERSION / profile


__all__ = ["build_parser", "main"]
