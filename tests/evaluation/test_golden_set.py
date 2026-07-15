from __future__ import annotations

from scripts.evaluate import evaluate


def test_golden_set_meets_all_phase_one_thresholds() -> None:
    report = evaluate()
    assert report["sample_count"] >= 20
    for name, threshold in report["thresholds"].items():
        assert report[name] >= threshold, f"{name} below {threshold}"
