from __future__ import annotations

import tracemalloc
from pathlib import Path

import pytest

from graphrag_data_factory.factory import DatasetFactory
from graphrag_data_factory.models import DatasetProfile
from graphrag_data_factory.streaming import (
    ResumableJsonlWriter,
    StreamingCheckpointError,
)
from graphrag_data_factory.streaming_factory import (
    StreamingDerivedFactory,
    StreamingEventFactory,
    StreamingProfileExporter,
)


def _records(start: int, stop: int) -> list[dict[str, object]]:
    return [
        {
            "index": index,
            "is_synthetic": True,
            "payload": f"deterministic-{index:06d}",
        }
        for index in range(start, stop)
    ]


@pytest.mark.fault
def test_streaming_writer_resumes_from_last_committed_batch(tmp_path: Path) -> None:
    interrupted = tmp_path / "interrupted"
    writer = ResumableJsonlWriter(
        interrupted,
        dataset_id="synthetic-test-resume",
        profile_sha256="a" * 64,
        batch_size=7,
    )
    with pytest.raises(RuntimeError, match="injected streaming interruption"):
        writer.write_records(
            record_type="sample",
            relative_path="sample.jsonl",
            total_records=31,
            produce=_records,
            fail_after_committed_batches=2,
        )

    checkpoint = writer.checkpoint
    assert checkpoint.active_file is not None
    assert checkpoint.active_file.committed_records == 14
    partial = interrupted / "sample.jsonl.part"
    with partial.open("ab") as stream:
        stream.write(b"uncommitted-tail")

    resumed = ResumableJsonlWriter(
        interrupted,
        dataset_id="synthetic-test-resume",
        profile_sha256="a" * 64,
        batch_size=5,
        resume=True,
    )
    entry = resumed.write_records(
        record_type="sample",
        relative_path="sample.jsonl",
        total_records=31,
        produce=_records,
    )

    clean = tmp_path / "clean"
    clean_entry = ResumableJsonlWriter(
        clean,
        dataset_id="synthetic-test-resume",
        profile_sha256="a" * 64,
        batch_size=11,
    ).write_records(
        record_type="sample",
        relative_path="sample.jsonl",
        total_records=31,
        produce=_records,
    )

    assert (interrupted / "sample.jsonl").read_bytes() == (clean / "sample.jsonl").read_bytes()
    assert entry.sha256 == clean_entry.sha256
    assert entry.records == 31
    assert not partial.exists()
    assert resumed.checkpoint.active_file is None
    assert resumed.checkpoint.completed_files["sample"].records == 31


@pytest.mark.invariant
def test_streaming_writer_rejects_checkpoint_identity_or_prefix_tampering(
    tmp_path: Path,
) -> None:
    root = tmp_path / "dataset"
    writer = ResumableJsonlWriter(
        root,
        dataset_id="synthetic-test-identity",
        profile_sha256="b" * 64,
        batch_size=3,
    )
    with pytest.raises(RuntimeError, match="injected streaming interruption"):
        writer.write_records(
            record_type="sample",
            relative_path="sample.jsonl",
            total_records=10,
            produce=_records,
            fail_after_committed_batches=1,
        )

    with pytest.raises(StreamingCheckpointError, match="dataset identity"):
        ResumableJsonlWriter(
            root,
            dataset_id="synthetic-test-other",
            profile_sha256="b" * 64,
            batch_size=3,
            resume=True,
        )

    partial = root / "sample.jsonl.part"
    payload = bytearray(partial.read_bytes())
    payload[0] = ord("X")
    partial.write_bytes(payload)
    with pytest.raises(StreamingCheckpointError, match="committed prefix checksum"):
        ResumableJsonlWriter(
            root,
            dataset_id="synthetic-test-identity",
            profile_sha256="b" * 64,
            batch_size=3,
            resume=True,
        )


@pytest.mark.contract
def test_streaming_writer_requires_explicit_resume_for_existing_checkpoint(tmp_path: Path) -> None:
    root = tmp_path / "dataset"
    ResumableJsonlWriter(
        root,
        dataset_id="synthetic-test-explicit-resume",
        profile_sha256="c" * 64,
        batch_size=2,
    )
    with pytest.raises(StreamingCheckpointError, match="pass resume explicitly"):
        ResumableJsonlWriter(
            root,
            dataset_id="synthetic-test-explicit-resume",
            profile_sha256="c" * 64,
            batch_size=2,
        )


@pytest.mark.invariant
def test_streamed_event_products_match_the_existing_ci_oracle(
    ci_profile: tuple[DatasetProfile, Path],
) -> None:
    profile, _ = ci_profile
    factory = DatasetFactory(profile)
    bundle = factory.generate()
    streamed = StreamingEventFactory(factory)

    assert tuple(streamed.events(0, profile.counts.events)) == bundle.records["event"]
    assert (
        tuple(streamed.deliveries(0, profile.counts.event_deliveries))
        == bundle.records["event_delivery"]
    )
    assert (
        tuple(streamed.replay_expectations(0, streamed.replay_expectation_count))
        == bundle.records["replay_expectation"]
    )
    assert tuple(streamed.dlq_repairs(0, streamed.dlq_repair_count)) == bundle.records["dlq_repair"]


@pytest.mark.invariant
def test_streamed_derived_products_match_the_existing_ci_oracle(
    ci_profile: tuple[DatasetProfile, Path],
) -> None:
    profile, _ = ci_profile
    factory = DatasetFactory(profile)
    bundle = factory.generate()
    streamed = StreamingDerivedFactory(factory)

    for record_type in (
        "access_decision",
        "order_oracle",
        "logistics_oracle",
        "refund_lifecycle",
        "evaluation_case",
        "memory_case",
        "golden_candidate",
        "security_case",
        "fault_schedule",
    ):
        count = streamed.count(record_type)
        assert tuple(streamed.produce(record_type, 0, count)) == bundle.records[record_type]


@pytest.mark.fault
def test_streaming_profile_export_resumes_without_publishing_partial_data(
    tmp_path: Path,
    ci_profile: tuple[DatasetProfile, Path],
) -> None:
    profile, profile_path = ci_profile
    profile = profile.model_copy(update={"requires_explicit_large_flag": True})
    target = tmp_path / "resumed"
    exporter = StreamingProfileExporter()

    with pytest.raises(RuntimeError, match="injected streaming interruption"):
        exporter.export(
            profile,
            profile_path=profile_path,
            target=target,
            fail_after_file="event",
        )

    assert not target.exists()
    staging = tuple(tmp_path.glob(".resumed.streaming-*"))
    assert len(staging) == 1
    assert (staging[0] / ".generation-state.json").is_file()

    exporter.export(
        profile,
        profile_path=profile_path,
        target=target,
        resume=True,
    )

    assert target.is_dir()
    assert not staging[0].exists()
    assert not (target / ".generation-state.json").exists()


@pytest.mark.invariant
def test_streaming_writer_memory_does_not_scale_with_record_count(tmp_path: Path) -> None:
    writer = ResumableJsonlWriter(
        tmp_path / "bounded",
        dataset_id="synthetic-test-bounded-memory",
        profile_sha256="d" * 64,
        batch_size=1_000,
    )
    tracemalloc.start()
    try:
        writer.write_records(
            record_type="sample",
            relative_path="sample.jsonl",
            total_records=100_000,
            produce=_records,
        )
        _, peak_bytes = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()

    assert peak_bytes < 16 * 1024 * 1024
