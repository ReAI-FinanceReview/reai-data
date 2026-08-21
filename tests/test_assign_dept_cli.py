"""Tests for scripts/assign_dept.py (Issue #40)

이 스크립트는 얇은 래퍼지만 두 가지를 혼자 결정한다: 대상 범위(하루치냐 전량이냐)와
Airflow BashOperator 가 읽는 exit code 다. 둘 다 회귀해도 다른 테스트는 통과한다.

``scripts`` 는 패키지가 아니므로 모듈을 파일 경로로 적재한다. 같은 방식이
``tests/test_eval_assignment_cli.py`` 에도 쓰인다.
"""

import importlib.util
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from src.gold.assigner_ids import ASSIGNER_LLM, ASSIGNER_RULE

ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = ROOT / "scripts" / "assign_dept.py"


def _load_cli():
    spec = importlib.util.spec_from_file_location("_assign_dept_cli", SCRIPT_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def cli():
    return _load_cli()


class TestScopeSelection:
    def test_requires_an_explicit_scope(self, cli):
        """날짜 생략이 곧 전량 처리이면 플래그 하나 빠뜨린 실행이 유료 전 이력 스윕이 된다."""
        with pytest.raises(SystemExit):
            cli.build_arg_parser().parse_args(["--assigner", ASSIGNER_LLM])

    def test_date_and_backfill_are_mutually_exclusive(self, cli):
        with pytest.raises(SystemExit):
            cli.build_arg_parser().parse_args(["--date", "2026-08-20", "--backfill"])

    def test_backfill_passes_no_target_date(self, cli):
        with patch.object(cli, "run_dept_assign") as run:
            run.return_value = MagicMock(status="success", as_dict=lambda: {})

            cli.main(["--backfill", "--assigner", ASSIGNER_LLM, "--limit", "10"])

        kwargs = run.call_args.kwargs
        assert kwargs["target_date"] is None
        assert kwargs["assigners"] == [ASSIGNER_LLM]
        assert kwargs["limit"] == 10


class TestArgumentWiring:
    def test_arguments_map_to_the_step_call(self, cli):
        with patch.object(cli, "run_dept_assign") as run:
            run.return_value = MagicMock(status="success", as_dict=lambda: {})

            cli.main(
                [
                    "--date", "2026-08-20",
                    "--batch-size", "10",
                    "--reassign",
                    "--assigner", ASSIGNER_RULE,
                    "--assigner", ASSIGNER_LLM,
                ]
            )

        kwargs = run.call_args.kwargs
        assert kwargs["target_date"] == "2026-08-20"
        assert kwargs["batch_size"] == 10
        assert kwargs["reassign"] is True
        assert kwargs["assigners"] == [ASSIGNER_RULE, ASSIGNER_LLM]

    def test_assigner_defaults_to_none_meaning_both(self, cli):
        """빈 리스트로 바뀌면 배정기를 하나도 돌지 않고 성공을 보고한다."""
        with patch.object(cli, "run_dept_assign") as run:
            run.return_value = MagicMock(status="success", as_dict=lambda: {})

            cli.main(["--date", "2026-08-20"])

        assert run.call_args.kwargs["assigners"] is None


class TestExitCode:
    def test_success_returns_zero(self, cli):
        with patch.object(cli, "run_dept_assign") as run:
            run.return_value = MagicMock(status="success", as_dict=lambda: {})

            assert cli.main(["--date", "2026-08-20"]) == 0

    def test_failure_returns_one(self, cli):
        """이 값이 Airflow 태스크의 성공/실패 신호 그 자체다."""
        with patch.object(cli, "run_dept_assign") as run:
            run.return_value = MagicMock(status="failed", as_dict=lambda: {})

            assert cli.main(["--date", "2026-08-20"]) == 1
