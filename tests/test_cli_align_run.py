"""CLI tests for align-run command."""

from __future__ import annotations

from ingestion import cli


def test_align_run_calls_pipeline(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def _fake_build_aligned_from_raw_run(**kwargs):
        captured.update(kwargs)
        return "data/interim/aligned_records.json"

    monkeypatch.setattr(
        cli, "build_aligned_from_raw_run", _fake_build_aligned_from_raw_run
    )

    exit_code = cli.main(
        [
            "align-run",
            "--raw-run-log",
            "data/raw/raw_ingestion_run_20250101T000000Z.json",
            "--output-json",
            "data/interim/aligned_records.json",
            "--duplicate-policy",
            "first",
        ]
    )

    assert exit_code == 0
    assert (
        captured["raw_run_log_path"]
        == "data/raw/raw_ingestion_run_20250101T000000Z.json"
    )
    assert captured["output_json_path"] == "data/interim/aligned_records.json"
    assert captured["duplicate_policy"] == "first"
