"""CLI tests for process-run command."""

from __future__ import annotations

import json
from pathlib import Path

from ingestion import cli


class _FakeResult:
    def __init__(self) -> None:
        self.dataset_json_path = "data/processed/f.dataset.json"
        self.report_json_path = "data/processed/f.missingness.json"
        self.parquet_path = "data/processed/f.parquet"
        self.metadata_path = "data/processed/f.metadata.json"
        self.validation_issue_count = 1


def test_process_run_calls_pipeline(monkeypatch, tmp_path: Path) -> None:
    input_path = tmp_path / "aligned.json"
    input_path.write_text(
        json.dumps([{"minute_utc": "2025-01-01T00:00:00Z"}]), encoding="utf-8"
    )

    captured: dict[str, object] = {}

    def _fake_run_processed_pipeline(records, **kwargs):
        captured["records"] = records
        captured.update(kwargs)
        return _FakeResult()

    monkeypatch.setattr(cli, "run_processed_pipeline", _fake_run_processed_pipeline)

    exit_code = cli.main(
        [
            "process-run",
            "--input-json",
            str(input_path),
            "--output-dir",
            "data/processed",
            "--dataset-name",
            "aligned_dataset",
            "--realized-vol-window",
            "5",
            "--annualization-minutes",
            "100",
            "--fail-on-warnings",
        ]
    )

    assert exit_code == 0
    assert isinstance(captured["records"], list)
    assert captured["output_dir"] == "data/processed"
    assert captured["dataset_name"] == "aligned_dataset"
    assert captured["realized_vol_window"] == 5
    assert captured["annualization_minutes"] == 100
    assert captured["fail_on_warnings"] is True
