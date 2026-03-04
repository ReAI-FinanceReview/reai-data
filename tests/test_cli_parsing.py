from pathlib import Path
import sys

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


def test_run_steps_handles_unknown_step():
    results = run_steps(["unknown"])
    assert results[-1].status == "failed"
    assert "unknown" in results[-1].message
