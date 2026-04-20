from pathlib import Path
import sys
from unittest.mock import MagicMock, patch

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.pipeline.cli import build_arg_parser  # noqa: E402
from src.pipeline.steps import run_steps  # noqa: E402


def test_cli_parses_steps_and_batch_size():
    parser = build_arg_parser()
    args = parser.parse_args(["--steps", "crawl,embed", "--batch-size", "50"])
    assert args.steps == "crawl,embed"
    assert args.batch_size == 50


def test_cli_parses_target_date():
    parser = build_arg_parser()
    args = parser.parse_args(["--steps", "gold,aggregate", "--target-date", "2025-01-15"])

    assert args.target_date == "2025-01-15"


def test_cli_rejects_invalid_target_date():
    parser = build_arg_parser()

    with pytest.raises(SystemExit):
        parser.parse_args(["--steps", "gold,aggregate", "--target-date", "today"])


def test_run_steps_handles_unknown_step():
    results = run_steps(["unknown"])
    assert results[-1].status == "failed"
    assert "unknown" in results[-1].message


@patch("src.pipeline.steps.run_aggregate")
@patch("src.pipeline.steps.run_gold")
def test_run_steps_passes_target_date_to_gold_and_aggregate(mock_run_gold, mock_run_aggregate):
    mock_run_gold.return_value = MagicMock(status="success")
    mock_run_aggregate.return_value = MagicMock(status="success")

    run_steps(["gold", "aggregate"], target_date="2025-01-15")

    assert mock_run_gold.call_args.kwargs["target_date"] == "2025-01-15"
    assert mock_run_aggregate.call_args.kwargs["target_date"] == "2025-01-15"


@patch("src.pipeline.steps.run_aggregate")
@patch("src.pipeline.steps.run_gold")
def test_run_steps_passes_default_target_date_to_gold_and_aggregate(mock_run_gold, mock_run_aggregate):
    mock_run_gold.return_value = MagicMock(status="success")
    mock_run_aggregate.return_value = MagicMock(status="success")

    run_steps(["gold", "aggregate"])

    assert mock_run_gold.call_args.kwargs["target_date"] is None
    assert mock_run_aggregate.call_args.kwargs["target_date"] is None
