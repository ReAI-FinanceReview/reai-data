"""Tests for scripts/eval_assignment.py (Issue #40)

이 스크립트는 저장소의 다른 `scripts/*.py` 와 달리 순수 로직을 직접 담고 있어
(경로 우선순위, 적재 현황 산술) 지금까지 테스트가 없었다. 리뷰에서 두 라운드
연속 지적된 항목이다.

`scripts/` 는 패키지가 아니므로 모듈을 파일 경로로 적재한다. 같은 방식이
`tests/test_gold_dept_assigner.py` 의 alembic 리비전 적재에도 쓰인다.
"""

import importlib.util
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = ROOT / "scripts" / "eval_assignment.py"


def _load_cli():
    spec = importlib.util.spec_from_file_location("_eval_assignment_cli", SCRIPT_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def cli():
    return _load_cli()


class TestResolveLabelsPath:
    def test_cli_argument_wins(self, cli, tmp_path, monkeypatch):
        monkeypatch.setenv(cli.LABELS_PATH_ENV, str(tmp_path / "env.csv"))

        assert cli.resolve_labels_path("/explicit.csv") == Path("/explicit.csv")

    def test_env_var_used_when_no_argument(self, cli, tmp_path, monkeypatch):
        monkeypatch.setenv(cli.LABELS_PATH_ENV, str(tmp_path / "env.csv"))

        assert cli.resolve_labels_path(None) == tmp_path / "env.csv"

    def test_falls_back_to_default_under_repo_root(self, cli, monkeypatch):
        monkeypatch.delenv(cli.LABELS_PATH_ENV, raising=False)

        resolved = cli.resolve_labels_path(None)

        assert resolved == cli.ROOT / cli.DEFAULT_LABELS_PATH
        # 기본 경로는 git 이 무시하는 곳이어야 한다. 실데이터가 담기기 때문이다.
        assert "data/labels" in str(cli.DEFAULT_LABELS_PATH).replace("\\", "/")


class TestFormatCoverage:
    def test_counts_and_rate(self, cli):
        out = cli.format_coverage(total=100, failed=10, unclassified=40)

        assert "assigned         50" in out
        assert "50.0%" in out

    def test_empty_table_does_not_divide_by_zero(self, cli):
        assert "0.0%" in cli.format_coverage(total=0, failed=0, unclassified=0)


class TestMainFailureModes:
    def test_missing_file_returns_1(self, cli, tmp_path, capsys):
        rc = cli.main(["--labels", str(tmp_path / "nope.csv")])

        assert rc == 1
        assert "없다" in capsys.readouterr().err

    def test_malformed_csv_reports_instead_of_traceback(self, cli, tmp_path, capsys):
        bad = tmp_path / "bad.csv"
        bad.write_text("no,review_id\n1,rid-a\n", encoding="utf-8")

        rc = cli.main(["--labels", str(bad)])

        assert rc == 1
        assert "읽을 수 없다" in capsys.readouterr().err

    def test_unlabeled_sheet_returns_1(self, cli, tmp_path, capsys):
        sheet = tmp_path / "empty.csv"
        sheet.write_text(
            "no,review_id,service,platform,rating,review_text,assigned_dept,"
            "stopped_at_parent,ambiguous,memo\n"
            "1,rid-a,신한,PLAYSTORE,1,본문,,,,\n",
            encoding="utf-8",
        )

        rc = cli.main(["--labels", str(sheet)])

        assert rc == 1
        assert "비어 있다" in capsys.readouterr().err


class TestAssignerDefault:
    def test_defaults_to_the_rule_assigner(self, cli, tmp_path, monkeypatch, capsys):
        """생략 시 '전체'로 두면 규칙/LLM 행이 섞여 리포트가 비결정적이 된다.

        DB 없이 확인하기 위해, 라벨을 읽은 뒤 DB 접속 직전까지만 진행시키고
        fetch_assignments 가 받은 assigner 인자를 가로챈다.
        """
        from src.gold.dept_assigner import ASSIGNER_RULE

        sheet = tmp_path / "labels.csv"
        sheet.write_text(
            "no,review_id,service,platform,rating,review_text,assigned_dept,"
            "stopped_at_parent,ambiguous,memo\n"
            "1,rid-a,신한,PLAYSTORE,1,본문,1-1,,,\n",
            encoding="utf-8",
        )

        seen = {}

        def _fake_fetch(session, review_ids, assigner=None):
            seen["assigner"] = assigner
            raise SystemExit(0)

        monkeypatch.setattr(cli, "fetch_assignments", _fake_fetch)
        monkeypatch.setattr(cli, "DatabaseConnector", lambda *a, **k: _StubConnector())

        with pytest.raises(SystemExit):
            cli.main(["--labels", str(sheet)])

        assert seen["assigner"] == ASSIGNER_RULE


class _StubConnector:
    def get_session(self):
        return _StubSession()


class _StubSession:
    def execute(self, *a, **k):
        raise AssertionError("fetch_assignments 보다 먼저 실행되면 안 된다")

    def close(self):
        pass
