"""Unit tests for GoldOrchestrator."""

from datetime import UTC, date, datetime
from unittest.mock import MagicMock, patch
from uuid import uuid4

import pytest

from src.models.enums import ProcessingStatusType


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_row(review_id):
    row = MagicMock()
    row.review_id = review_id
    return row


def _make_orchestrator():
    """Build a GoldOrchestrator without calling __init__ (no DB needed)."""
    with patch("src.gold.orchestrator.DatabaseConnector"):
        from src.gold.orchestrator import GoldOrchestrator
        orch = GoldOrchestrator.__new__(GoldOrchestrator)
    orch.logger = MagicMock()
    orch.db_connector = MagicMock()
    orch._embedding = MagicMock()
    orch._absa = MagicMock()
    orch._action = MagicMock()
    return orch


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def mock_session():
    session = MagicMock()
    session.commit = MagicMock()
    session.rollback = MagicMock()
    session.close = MagicMock()
    return session


@pytest.fixture
def mock_rmi():
    rmi = MagicMock()
    rmi.retry_count = 0
    return rmi


# ---------------------------------------------------------------------------
# _fetch_pending_ids
# ---------------------------------------------------------------------------

class TestFetchPendingIds:
    def test_returns_pending_ids(self, mock_session):
        ids = [uuid4(), uuid4()]
        mock_session.query.return_value.filter.return_value.all.return_value = [
            _make_row(ids[0]),
            _make_row(ids[1]),
        ]
        orch = _make_orchestrator()
        result = orch._fetch_pending_ids(mock_session, limit=None)
        assert len(result) == 2

    def test_respects_limit(self, mock_session):
        mock_session.query.return_value.filter.return_value.limit.return_value.all.return_value = [
            _make_row(uuid4())
        ]
        orch = _make_orchestrator()
        result = orch._fetch_pending_ids(mock_session, limit=1)
        assert len(result) == 1

    def test_applies_target_date_filter(self, mock_session):
        filtered_query = mock_session.query.return_value.filter.return_value
        filtered_query.filter.return_value.all.return_value = [_make_row(uuid4())]

        orch = _make_orchestrator()
        result = orch._fetch_pending_ids(mock_session, limit=None, target_date=date(2025, 1, 15))

        assert len(result) == 1
        assert filtered_query.filter.called

        lower_bound, upper_bound = filtered_query.filter.call_args.args
        assert lower_bound.right.value == datetime(2025, 1, 15, 0, 0, tzinfo=UTC)
        assert upper_bound.right.value == datetime(2025, 1, 16, 0, 0, tzinfo=UTC)


# ---------------------------------------------------------------------------
# _process_one
# ---------------------------------------------------------------------------

class TestProcessOne:
    def test_all_modules_succeed_sets_analyzed(self, mock_session, mock_rmi):
        orch = _make_orchestrator()
        orch._embedding.process.return_value = True
        orch._absa.process.return_value = True
        orch._action.process.return_value = True

        review_id = uuid4()
        mock_session.get.return_value = mock_rmi

        result = orch._process_one(mock_session, review_id)

        assert result == ProcessingStatusType.ANALYZED
        assert mock_rmi.processing_status == ProcessingStatusType.ANALYZED
        assert mock_rmi.error_message is None

    def test_embedding_failure_sets_failed(self, mock_session, mock_rmi):
        orch = _make_orchestrator()
        orch._embedding.process.return_value = False

        review_id = uuid4()
        mock_session.get.return_value = mock_rmi

        result = orch._process_one(mock_session, review_id)

        assert result == ProcessingStatusType.FAILED
        assert mock_rmi.processing_status == ProcessingStatusType.FAILED
        assert mock_rmi.retry_count == 1

    def test_absa_failure_sets_failed(self, mock_session, mock_rmi):
        orch = _make_orchestrator()
        orch._embedding.process.return_value = True
        orch._absa.process.return_value = False

        review_id = uuid4()
        mock_session.get.return_value = mock_rmi

        result = orch._process_one(mock_session, review_id)

        assert result == ProcessingStatusType.FAILED
        assert mock_rmi.processing_status == ProcessingStatusType.FAILED

    def test_action_failure_sets_failed(self, mock_session, mock_rmi):
        orch = _make_orchestrator()
        orch._embedding.process.return_value = True
        orch._absa.process.return_value = True
        orch._action.process.return_value = False

        review_id = uuid4()
        mock_session.get.return_value = mock_rmi

        result = orch._process_one(mock_session, review_id)

        assert result == ProcessingStatusType.FAILED
        assert mock_rmi.processing_status == ProcessingStatusType.FAILED

    def test_exception_increments_retry_count(self, mock_session, mock_rmi):
        orch = _make_orchestrator()
        orch._embedding.process.side_effect = RuntimeError("network error")

        review_id = uuid4()
        mock_rmi.retry_count = 1
        mock_session.get.return_value = mock_rmi

        result = orch._process_one(mock_session, review_id)

        assert result == ProcessingStatusType.FAILED
        assert mock_rmi.retry_count == 2

    def test_missing_rmi_record_logs_error(self, mock_session):
        orch = _make_orchestrator()
        orch._embedding.process.return_value = True
        orch._absa.process.return_value = True
        orch._action.process.return_value = True

        review_id = uuid4()
        mock_session.get.return_value = None

        result = orch._process_one(mock_session, review_id)
        assert result == ProcessingStatusType.ANALYZED
        orch.logger.error.assert_called_once()


# ---------------------------------------------------------------------------
# run()
# ---------------------------------------------------------------------------

class TestRun:
    def _make_orch_with_session(self, mock_session, review_ids):
        orch = _make_orchestrator()
        orch.db_connector.get_session.return_value = mock_session
        mock_session.query.return_value.filter.return_value.all.return_value = [
            _make_row(rid) for rid in review_ids
        ]
        return orch

    def test_no_pending_returns_zeros(self, mock_session):
        orch = self._make_orch_with_session(mock_session, [])
        result = orch.run()
        assert result == {"total": 0, "analyzed": 0, "skipped": 0, "failed": 0}
        mock_session.commit.assert_not_called()

    def test_all_success_counts_correctly(self, mock_session, mock_rmi):
        review_ids = [uuid4(), uuid4()]
        orch = self._make_orch_with_session(mock_session, review_ids)
        orch._embedding.process.return_value = True
        orch._absa.process.return_value = True
        orch._action.process.return_value = True
        mock_session.get.return_value = mock_rmi

        result = orch.run(batch_size=10)

        assert result["total"] == 2
        assert result["analyzed"] == 2
        assert result["failed"] == 0
        mock_session.commit.assert_called()

    def test_partial_failure_counted(self, mock_session, mock_rmi):
        review_ids = [uuid4(), uuid4()]
        orch = self._make_orch_with_session(mock_session, review_ids)
        orch._embedding.process.side_effect = [True, False]
        orch._absa.process.return_value = True
        orch._action.process.return_value = True
        mock_session.get.return_value = mock_rmi

        result = orch.run(batch_size=10)

        assert result["total"] == 2
        assert result["analyzed"] == 1
        assert result["failed"] == 1

    def test_session_closed_on_success(self, mock_session, mock_rmi):
        review_ids = [uuid4()]
        orch = self._make_orch_with_session(mock_session, review_ids)
        orch._embedding.process.return_value = True
        orch._absa.process.return_value = True
        orch._action.process.return_value = True
        mock_session.get.return_value = mock_rmi

        orch.run()
        mock_session.close.assert_called_once()

    def test_session_rolled_back_on_exception(self, mock_session):
        orch = _make_orchestrator()
        orch.db_connector.get_session.return_value = mock_session
        mock_session.query.side_effect = RuntimeError("db error")

        with pytest.raises(RuntimeError):
            orch.run()

        mock_session.rollback.assert_called_once()
        mock_session.close.assert_called_once()


# ---------------------------------------------------------------------------
# SKIPPED 종결 상태 (issue #94) — 실제 DB · 실제 분석기
# ---------------------------------------------------------------------------
# 오케스트레이터는 자기 세션으로 커밋하므로 롤백 세션 픽스처를 쓸 수 없다. 다른 테스트와
# 겹치지 않는 과거 날짜에 커밋하고 finally 에서 지운다.

_SKIP_TARGET_DATE = date(2001, 2, 3)


def _seed_review(connection, *, app_id, refined_text, status, retry_count, embed):
    from sqlalchemy import text
    from uuid6 import uuid7

    review_id = uuid7()
    connection.execute(
        text(
            "INSERT INTO review_master_index "
            "(review_id, app_id, platform_review_id, platform_type, review_created_at, "
            " processing_status, retry_count) "
            "VALUES (:review_id, :app_id, :prid, 'APPSTORE', :created_at, :status, :retry_count)"
        ),
        {
            "review_id": review_id,
            "app_id": app_id,
            "prid": f"orch-skip-{review_id}",
            "created_at": datetime(2001, 2, 3, 9, 30, tzinfo=UTC),
            "status": status,
            "retry_count": retry_count,
        },
    )
    connection.execute(
        text(
            "INSERT INTO reviews_preprocessed (review_id, platform_review_id, refined_text) "
            "VALUES (:review_id, :prid, :refined_text)"
        ),
        {"review_id": review_id, "prid": f"orch-skip-{review_id}", "refined_text": refined_text},
    )
    if embed:
        # 임베딩이 이미 있으면 생성기가 API 를 부르지 않는다
        connection.execute(
            text(
                "INSERT INTO review_embeddings (review_id, source_content_type, model_name, vector) "
                "VALUES (:review_id, 'preprocessed', 'text-embedding-3-small', :vector)"
            ),
            {"review_id": review_id, "vector": "[" + ",".join(["0.001"] * 1536) + "]"},
        )
    return review_id


def _cleanup_skip_probe(engine, app_id):
    from sqlalchemy import text

    with engine.begin() as connection:
        for table in ("review_aspects", "review_embeddings", "reviews_preprocessed"):
            connection.execute(
                text(
                    f"DELETE FROM {table} WHERE review_id IN "
                    "(SELECT review_id FROM review_master_index WHERE app_id = :app_id)"
                ),
                {"app_id": app_id},
            )
        connection.execute(
            text("DELETE FROM review_master_index WHERE app_id = :app_id"), {"app_id": app_id}
        )
        connection.execute(text("DELETE FROM apps WHERE app_id = :app_id"), {"app_id": app_id})


@pytest.mark.requires_db
def test_unanalyzable_reviews_become_terminal_skipped(
    test_db_engine, test_db_url, test_db_schema, monkeypatch
):
    from sqlalchemy import text
    from uuid6 import uuid7

    from src.gold.orchestrator import GoldOrchestrator

    monkeypatch.setenv("DATABASE_URL", test_db_url)
    monkeypatch.setenv("OPENAI_API_KEY", "")
    app_id = uuid7()
    try:
        with test_db_engine.begin() as connection:
            connection.execute(
                text(
                    "INSERT INTO apps (app_id, platform_app_id, platform_type, name) "
                    "VALUES (:app_id, :pid, 'APPSTORE', 'Orchestrator Skip Test App')"
                ),
                {"app_id": app_id, "pid": f"orch.skip.{app_id}"},
            )
            empty = _seed_review(
                connection, app_id=app_id, refined_text="", status="CLEANED",
                retry_count=0, embed=False,
            )
            # 이번 이슈의 실데이터 모양: FAILED 로 1회 재시도된 영문 리뷰
            english = _seed_review(
                connection, app_id=app_id,
                refined_text="My app stopped working after updating.",
                status="FAILED", retry_count=1, embed=True,
            )
            noise = _seed_review(
                connection, app_id=app_id, refined_text="ㅇㅇ", status="CLEANED",
                retry_count=0, embed=True,
            )

        orchestrator = GoldOrchestrator()
        first = orchestrator.run(target_date=_SKIP_TARGET_DATE)
        assert first == {"total": 3, "analyzed": 0, "skipped": 3, "failed": 0}

        with test_db_engine.connect() as connection:
            rows = {
                row.review_id: row
                for row in connection.execute(
                    text(
                        "SELECT review_id, processing_status, error_message, retry_count "
                        "FROM review_master_index WHERE app_id = :app_id"
                    ),
                    {"app_id": app_id},
                )
            }
            aspect_rows = connection.execute(
                text(
                    "SELECT COUNT(*) FROM review_aspects WHERE review_id IN "
                    "(SELECT review_id FROM review_master_index WHERE app_id = :app_id)"
                ),
                {"app_id": app_id},
            ).scalar_one()
        assert {rid: rows[rid].error_message for rid in rows} == {
            empty: "skip:empty",
            english: "skip:non_korean",
            noise: "skip:no_aspect",
        }
        assert {row.processing_status for row in rows.values()} == {"SKIPPED"}
        # 실패가 아니므로 retry_count 를 올리지 않는다
        assert {rid: rows[rid].retry_count for rid in rows} == {empty: 0, english: 1, noise: 0}
        assert aspect_rows == 0

        # 종결 상태라 다시 집어 가지 않는다
        assert orchestrator.run(target_date=_SKIP_TARGET_DATE)["total"] == 0
    finally:
        _cleanup_skip_probe(test_db_engine, app_id)


@pytest.mark.requires_db
@pytest.mark.parametrize(
    ("texts", "expected_status"),
    [
        # 분석 제외 리뷰만 있는 날: 0건 분석이어도 단계 실패가 아니다
        (("",), "success"),
        # SKIPPED 는 텍스트만 보고 정해져 API 장애일에도 생긴다. 실패를 덮으면 안 된다
        (("", "로그인이 안돼요"), "failed"),
        (("로그인이 안돼요",), "failed"),
    ],
)
def test_run_gold_fails_only_when_nothing_was_analyzed_and_something_failed(
    test_db_engine, test_db_url, test_db_schema, monkeypatch, texts, expected_status
):
    """임베딩이 없는 한국어 리뷰는 API 키가 없으면 FAILED 가 된다 (장애 재현)."""
    from sqlalchemy import text
    from uuid6 import uuid7

    from src.pipeline.steps import run_gold

    monkeypatch.setenv("DATABASE_URL", test_db_url)
    # 빈 값으로 고정해 저장소 .env 가 실제 키를 채우지 못하게 한다 (load_dotenv 는 덮어쓰지 않음)
    monkeypatch.setenv("OPENAI_API_KEY", "")
    app_id = uuid7()
    try:
        with test_db_engine.begin() as connection:
            connection.execute(
                text(
                    "INSERT INTO apps (app_id, platform_app_id, platform_type, name) "
                    "VALUES (:app_id, :pid, 'APPSTORE', 'Run Gold Skip Test App')"
                ),
                {"app_id": app_id, "pid": f"run.gold.skip.{app_id}"},
            )
            for refined_text in texts:
                _seed_review(
                    connection, app_id=app_id, refined_text=refined_text, status="CLEANED",
                    retry_count=0, embed=False,
                )

        result = run_gold(target_date=_SKIP_TARGET_DATE.isoformat())

        assert result.status == expected_status, result.message
    finally:
        _cleanup_skip_probe(test_db_engine, app_id)
