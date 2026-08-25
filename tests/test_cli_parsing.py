from datetime import date, timedelta
import inspect
from pathlib import Path
import re
import sys
from unittest.mock import MagicMock, patch

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import src.pipeline.steps as steps_module  # noqa: E402
from src.pipeline.cli import _parse_steps, build_arg_parser  # noqa: E402
from src.pipeline.steps import RunResult, run_cleanse, run_steps  # noqa: E402


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


# ========================================
# 기본 --steps (#87)
# ========================================


def _registry_step_names() -> set[str]:
    """run_steps 가 실제로 디스패치하는 단계 이름."""
    src = inspect.getsource(steps_module.run_steps)
    names = set(re.findall(r'^\s+"(\w+)": lambda', src, flags=re.MULTILINE))
    # 정규식이 깨지면 빈 집합이 되어 아래 단언들이 조용히 통과한다. 앵커로 막는다.
    assert {"crawl", "gold", "aggregate"} <= names, names
    return names


def _help_step_names(parser) -> list[str]:
    """--steps help 문자열이 광고하는 단계 이름."""
    help_text = {a.dest: a.help for a in parser._actions}["steps"]
    listed = re.search(r"options: ([^;)]+)", help_text)
    assert listed, help_text
    return [name.strip() for name in listed.group(1).split(",")]


def _stub_steps(monkeypatch, *, keep: frozenset[str]) -> None:
    """keep 에 없는 run_* 을 성공 스텁으로 바꾼다. 실제 I/O 없이 디스패치만 본다."""
    for name in dir(steps_module):
        if not name.startswith("run_") or name in keep or name == "run_steps":
            continue
        monkeypatch.setattr(
            steps_module,
            name,
            lambda *a, _n=name, **k: RunResult(step=_n, status="success"),
        )


def test_default_steps_run_to_completion(monkeypatch):
    """인자 없이 실행하면 요청한 단계가 하나도 빠짐없이 실행된다.

    run_preprocess 만 실물로 남긴다. deprecated 단계가 기본값에 돌아오면
    run_steps 가 첫 실패에서 멈추므로 실행된 단계 목록이 짧아진다.
    """
    default_steps = _parse_steps(build_arg_parser().parse_args([]).steps)
    _stub_steps(monkeypatch, keep=frozenset({"run_preprocess"}))

    results = run_steps(default_steps)

    # 스텁은 레지스트리 키가 아니라 함수명을 step 에 넣으므로 이름이 아니라
    # "요청한 만큼 실행됐는가" 로 본다. 중단이 일어나면 목록이 짧아진다.
    assert len(results) == len(default_steps), [r.as_dict() for r in results]
    assert all(r.status == "success" for r in results), [r.as_dict() for r in results]


def test_default_steps_exclude_deprecated_preprocess():
    default_steps = _parse_steps(build_arg_parser().parse_args([]).steps)
    assert "preprocess" not in default_steps


def test_help_step_options_match_registry():
    """help 가 광고하는 목록과 실제 디스패치 가능한 단계가 양방향으로 일치한다."""
    parser = build_arg_parser()
    advertised = _help_step_names(parser)
    dispatchable = _registry_step_names()

    assert not set(advertised) - dispatchable, "help 에만 있는 단계"
    # preprocess 는 deprecated 라 목록 본문이 아니라 별도 문구로만 언급한다.
    assert not dispatchable - set(advertised) - {"preprocess"}, "레지스트리에만 있는 단계"


@patch("src.pipeline.steps.run_cleanse")
def test_run_steps_passes_target_date_to_cleanse(mock_run_cleanse):
    mock_run_cleanse.return_value = RunResult(step="cleanse", status="success")

    run_steps(["cleanse"], target_date="2025-01-15")

    assert mock_run_cleanse.call_args.args[0] == "2025-01-15"


@patch("src.utils.minio_client.MinIOClient")
@patch("src.utils.db_connector.DatabaseConnector")
@patch("src.processing.cleanse.ReviewCleaningPipeline")
def test_run_cleanse_defaults_to_yesterday(mock_pipeline, mock_db, mock_minio):
    """날짜 생략 시 scripts/cleanse_reviews.py 와 같은 어제를 처리한다."""
    result = run_cleanse()

    assert result.status == "success", result.as_dict()
    assert mock_pipeline.return_value.run.call_args.kwargs["target_date"] == (
        date.today() - timedelta(days=1)
    )


def test_run_cleanse_rejects_invalid_target_date():
    result = run_cleanse(target_date="today")

    assert result.status == "failed"
    assert "Expected YYYY-MM-DD" in result.message
