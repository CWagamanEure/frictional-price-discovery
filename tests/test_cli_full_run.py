"""CLI tests for full-run command wiring."""

from __future__ import annotations

from dataclasses import dataclass

from ingestion import cli


@dataclass
class _FakeRawResult:
    run_id: str
    files: dict[str, str]
    row_counts: dict[str, int]


@dataclass
class _FakeProcessedResult:
    dataset_json_path: str
    report_json_path: str
    parquet_path: str
    metadata_path: str
    validation_issue_count: int


@dataclass
class _FakeFullResult:
    raw_result: _FakeRawResult
    aligned_json_path: str
    processed_result: _FakeProcessedResult
    summary_json_path: str
    quality_issue_count: int


def test_full_run_calls_pipeline(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def _fake_run_full_pipeline(**kwargs):
        captured.update(kwargs)
        return _FakeFullResult(
            raw_result=_FakeRawResult(run_id="r", files={}, row_counts={}),
            aligned_json_path="data/interim/aligned.json",
            processed_result=_FakeProcessedResult(
                dataset_json_path="data/processed/f.json",
                report_json_path="data/processed/r.json",
                parquet_path="data/processed/p.parquet",
                metadata_path="data/processed/m.json",
                validation_issue_count=0,
            ),
            summary_json_path="data/processed/s.json",
            quality_issue_count=0,
        )

    monkeypatch.setattr(cli, "run_full_pipeline", _fake_run_full_pipeline)

    exit_code = cli.main(
        [
            "full-run",
            "--start-time-utc",
            "2025-01-01T00:00:00Z",
            "--end-time-utc",
            "2025-01-01T00:10:00Z",
            "--raw-output-dir",
            "data/raw",
            "--processed-output-dir",
            "data/processed",
            "--dataset-name",
            "aligned_dataset",
            "--rpc-mode",
            "feehistory",
            "--min-uniswap5-coverage",
            "0.8",
            "--max-uniswap30-stale-share",
            "0.9",
            "--fail-on-quality-warnings",
        ]
    )

    assert exit_code == 0
    assert captured["raw_output_dir"] == "data/raw"
    assert captured["processed_output_dir"] == "data/processed"
    assert captured["dataset_name"] == "aligned_dataset"
    assert captured["rpc_mode"] == "feehistory"
    assert captured["min_uniswap5_coverage"] == 0.8
    assert captured["max_uniswap30_stale_share"] == 0.9
    assert captured["fail_on_quality_warnings"] is True
