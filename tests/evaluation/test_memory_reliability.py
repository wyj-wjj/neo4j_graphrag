from __future__ import annotations

from scripts.evaluate_reliability import evaluate_reliability


def test_memory_reliability_golden_set_meets_phase_1_5_thresholds() -> None:
    report = evaluate_reliability()
    assert report["dataset"] == "memory-reliability-v1"
    assert report["sample_count"] == 4
    for name, threshold in report["minimums"].items():
        assert report[name] >= threshold, f"{name} below {threshold}"
    for name, threshold in report["maximums"].items():
        assert report[name] <= threshold, f"{name} above {threshold}"
