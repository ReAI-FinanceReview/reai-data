"""Unit tests for GoldAggregator."""

from datetime import date, timedelta
from unittest.mock import MagicMock, patch, call

import pytest


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


def _make_aggregator(mock_session):
    with patch("src.gold.aggregator.DatabaseConnector") as MockDB:
        MockDB.return_value.get_session.return_value = mock_session
        from src.gold.aggregator import GoldAggregator
        agg = GoldAggregator.__new__(GoldAggregator)
        agg.logger = MagicMock()
        agg.db_connector = MockDB.return_value
        return agg


# ---------------------------------------------------------------------------
# run()
# ---------------------------------------------------------------------------

class TestRun:
    def test_run_calls_all_four_upserts(self, mock_session):
        agg = _make_aggregator(mock_session)
        agg._upsert_fact_service_review_daily = MagicMock()
        agg._upsert_fact_service_aspect_daily = MagicMock()
        agg._upsert_fact_category_radar_scores = MagicMock()
        agg._upsert_srv_daily_review_list = MagicMock()
        agg._drop_old_partitions = MagicMock(return_value=0)

        target = date(2025, 1, 15)
        result = agg.run(target_date=target)

        agg._upsert_fact_service_review_daily.assert_called_once_with(mock_session, target)
        agg._upsert_fact_service_aspect_daily.assert_called_once_with(mock_session, target)
        agg._upsert_fact_category_radar_scores.assert_called_once_with(mock_session, target)
        agg._upsert_srv_daily_review_list.assert_called_once_with(mock_session, target)
        assert result["date"] == "2025-01-15"
        assert set(result["tables_updated"]) == {
            "fact_service_review_daily",
            "fact_service_aspect_daily",
            "fact_category_radar_scores",
            "srv_daily_review_list",
        }
        assert result["dropped_partitions"] == 0

    def test_run_defaults_to_today(self, mock_session):
        agg = _make_aggregator(mock_session)
        agg._upsert_fact_service_review_daily = MagicMock()
        agg._upsert_fact_service_aspect_daily = MagicMock()
        agg._upsert_fact_category_radar_scores = MagicMock()
        agg._upsert_srv_daily_review_list = MagicMock()
        agg._drop_old_partitions = MagicMock(return_value=0)

        result = agg.run(target_date=None)

        assert result["date"] == str(date.today())

    def test_run_commits_on_success(self, mock_session):
        agg = _make_aggregator(mock_session)
        for method in (
            "_upsert_fact_service_review_daily",
            "_upsert_fact_service_aspect_daily",
            "_upsert_fact_category_radar_scores",
            "_upsert_srv_daily_review_list",
            "_drop_old_partitions",
        ):
            setattr(agg, method, MagicMock(return_value=0))

        agg.run(target_date=date.today())

        # 집계 트랜잭션 1회 + TTL 트랜잭션 1회 = 2회
        assert mock_session.commit.call_count == 2
        assert mock_session.close.call_count == 2

    def test_run_rolls_back_on_exception(self, mock_session):
        agg = _make_aggregator(mock_session)
        agg._upsert_fact_service_review_daily = MagicMock(side_effect=RuntimeError("db error"))

        with pytest.raises(RuntimeError):
            agg.run(target_date=date.today())

        mock_session.rollback.assert_called_once()
        mock_session.close.assert_called_once()
        mock_session.commit.assert_not_called()


# ---------------------------------------------------------------------------
# run_range() / run_all()
# ---------------------------------------------------------------------------

class TestRunRange:
    def test_run_range_aggregates_only_bounded_analyzed_dates(self, mock_session):
        agg = _make_aggregator(mock_session)
        dates = [date(2025, 1, 13), date(2025, 1, 14), date(2025, 1, 15)]
        agg._fetch_analyzed_dates = MagicMock(return_value=dates)
        agg._upsert_fact_service_review_daily = MagicMock()
        agg._upsert_fact_service_aspect_daily = MagicMock()
        agg._upsert_fact_category_radar_scores = MagicMock()
        agg._upsert_srv_daily_review_list = MagicMock()
        agg._drop_old_partitions = MagicMock(return_value=2)

        result = agg.run_range(date(2025, 1, 13), date(2025, 1, 15))

        agg._fetch_analyzed_dates.assert_called_once_with(
            mock_session,
            start_date=date(2025, 1, 13),
            end_date=date(2025, 1, 15),
        )
        assert agg._upsert_fact_service_review_daily.call_count == 3
        assert agg._upsert_srv_daily_review_list.call_count == 3
        assert result["dates"] == ["2025-01-13", "2025-01-14", "2025-01-15"]
        assert result["failed_dates"] == []
        assert "fact_service_review_daily" in result["tables_updated"]
        assert result["dropped_partitions"] == 2

    def test_run_range_empty_returns_early(self, mock_session):
        agg = _make_aggregator(mock_session)
        agg._fetch_analyzed_dates = MagicMock(return_value=[])

        result = agg.run_range(date(2025, 1, 13), date(2025, 1, 15))

        assert result["dates"] == []
        assert result["failed_dates"] == []
        assert result["dropped_partitions"] == 0
        mock_session.commit.assert_not_called()

    def test_run_range_commits_per_date(self, mock_session):
        """날짜별 독립 커밋 3회 + TTL 커밋 1회 = 총 4회."""
        agg = _make_aggregator(mock_session)
        agg._fetch_analyzed_dates = MagicMock(
            return_value=[date(2025, 1, 13), date(2025, 1, 14), date(2025, 1, 15)]
        )
        for method in (
            "_upsert_fact_service_review_daily",
            "_upsert_fact_service_aspect_daily",
            "_upsert_fact_category_radar_scores",
            "_upsert_srv_daily_review_list",
            "_drop_old_partitions",
        ):
            setattr(agg, method, MagicMock(return_value=0))

        agg.run_range(date(2025, 1, 13), date(2025, 1, 15))

        assert mock_session.commit.call_count == 4  # 3 per-date + 1 TTL
        assert mock_session.close.call_count == 2   # 집계 세션 + TTL 세션

    def test_run_range_raises_if_any_date_failed(self, mock_session):
        """실패 날짜 존재 시 RuntimeError — DAG가 실패로 인식하도록."""
        agg = _make_aggregator(mock_session)
        dates = [date(2025, 1, 13), date(2025, 1, 14), date(2025, 1, 15)]
        agg._fetch_analyzed_dates = MagicMock(return_value=dates)

        def side_effect(session, d):
            if d == date(2025, 1, 14):
                raise RuntimeError("DB error on Jan 14")
        agg._upsert_fact_service_review_daily = MagicMock(side_effect=side_effect)
        agg._upsert_fact_service_aspect_daily = MagicMock()
        agg._upsert_fact_category_radar_scores = MagicMock()
        agg._upsert_srv_daily_review_list = MagicMock()

        with pytest.raises(RuntimeError, match="2025-01-14"):
            agg.run_range(date(2025, 1, 13), date(2025, 1, 15))

        # 성공 날짜는 이미 commit, 실패 날짜는 rollback — 부분 성공 보존됨
        assert mock_session.commit.call_count == 2
        assert mock_session.rollback.call_count == 1
        mock_session.close.assert_called_once()

    def test_run_range_rejects_reversed_bounds(self, mock_session):
        agg = _make_aggregator(mock_session)

        with pytest.raises(ValueError, match="start_date must be on or before end_date"):
            agg.run_range(date(2025, 1, 15), date(2025, 1, 13))


class TestRunAll:
    def test_run_all_delegates_to_full_range_fetch(self, mock_session):
        agg = _make_aggregator(mock_session)
        agg._fetch_analyzed_dates = MagicMock(return_value=[])

        result = agg.run_all()

        agg._fetch_analyzed_dates.assert_called_once_with(
            mock_session,
            start_date=date.min,
            end_date=date.max,
        )
        assert result["dates"] == []


# ---------------------------------------------------------------------------
# _ensure_partition() — #56 파티션 자동 생성
# ---------------------------------------------------------------------------

class TestEnsurePartition:
    def test_ensure_partition_executes_ddl(self, mock_session):
        agg = _make_aggregator(mock_session)
        agg._ensure_partition(mock_session, date(2025, 1, 15))
        mock_session.execute.assert_called_once()
        ddl_text = str(mock_session.execute.call_args[0][0])
        assert "srv_daily_review_list_2025_01_15" in ddl_text
        assert "PARTITION OF public.srv_daily_review_list" in ddl_text

    def test_ensure_partition_embeds_literal_date_bounds(self, mock_session):
        """FOR VALUES FROM/TO는 bind parameter 불가 — ISO 날짜 리터럴로 직접 삽입."""
        agg = _make_aggregator(mock_session)
        agg._ensure_partition(mock_session, date(2025, 1, 15))
        ddl_text = str(mock_session.execute.call_args[0][0])
        assert "2025-01-15" in ddl_text
        assert "2025-01-16" in ddl_text
        # 파라미터 dict 없이 단일 인자로 호출됨
        assert len(mock_session.execute.call_args[0]) == 1

    def test_ensure_partition_month_boundary(self, mock_session):
        agg = _make_aggregator(mock_session)
        agg._ensure_partition(mock_session, date(2025, 1, 31))
        ddl_text = str(mock_session.execute.call_args[0][0])
        assert "2025-01-31" in ddl_text
        assert "2025-02-01" in ddl_text

    def test_ensure_partition_includes_public_schema(self, mock_session):
        agg = _make_aggregator(mock_session)
        agg._ensure_partition(mock_session, date(2025, 1, 15))
        ddl_text = str(mock_session.execute.call_args[0][0])
        assert "public.srv_daily_review_list_2025_01_15" in ddl_text


# ---------------------------------------------------------------------------
# SQL execution (smoke tests — verify session.execute is called)
# ---------------------------------------------------------------------------

class TestUpsertQueries:
    def test_fact_service_review_daily_executes_sql(self, mock_session):
        agg = _make_aggregator(mock_session)
        agg._upsert_fact_service_review_daily(mock_session, date(2025, 1, 15))
        mock_session.execute.assert_called_once()
        args = mock_session.execute.call_args
        assert args[0][1] == {"target_date": date(2025, 1, 15)}

    def test_fact_service_aspect_daily_executes_sql(self, mock_session):
        agg = _make_aggregator(mock_session)
        agg._upsert_fact_service_aspect_daily(mock_session, date(2025, 1, 15))
        mock_session.execute.assert_called_once()

    def test_fact_category_radar_scores_executes_sql(self, mock_session):
        agg = _make_aggregator(mock_session)
        agg._upsert_fact_category_radar_scores(mock_session, date(2025, 1, 15))
        mock_session.execute.assert_called_once()

    def test_srv_daily_review_list_executes_sql(self, mock_session):
        agg = _make_aggregator(mock_session)
        agg._upsert_srv_daily_review_list(mock_session, date(2025, 1, 15))
        # _ensure_partition(DDL) + INSERT = 2 calls
        assert mock_session.execute.call_count == 2


# ---------------------------------------------------------------------------
# _drop_old_partitions() — #56 TTL 파티션 삭제
# ---------------------------------------------------------------------------

class TestDropOldPartitions:
    def _make_session_with_partitions(self, partition_names):
        """mock_session.execute(catalog_sql).fetchall() 이 partition_names 반환하도록 설정."""
        session = MagicMock()
        fetch_result = MagicMock()
        fetch_result.fetchall.return_value = [(name,) for name in partition_names]
        session.execute.return_value = fetch_result
        return session

    def test_drops_partitions_older_than_retention(self):
        today = date.today()
        old_date = today - timedelta(days=15)
        old_name = f"srv_daily_review_list_{old_date.strftime('%Y_%m_%d')}"
        session = self._make_session_with_partitions([old_name])
        agg = _make_aggregator(MagicMock())

        dropped = agg._drop_old_partitions(session, retention_days=14)

        assert dropped == 1
        drop_calls = [
            str(c[0][0]) for c in session.execute.call_args_list[1:]
        ]
        assert any(old_name in call for call in drop_calls)

    def test_skips_recent_partitions(self):
        today = date.today()
        recent_date = today - timedelta(days=5)
        recent_name = f"srv_daily_review_list_{recent_date.strftime('%Y_%m_%d')}"
        session = self._make_session_with_partitions([recent_name])
        agg = _make_aggregator(MagicMock())

        dropped = agg._drop_old_partitions(session, retention_days=14)

        assert dropped == 0
        # catalog 조회 1회만 호출, DROP 없음
        assert session.execute.call_count == 1

    def test_returns_dropped_count(self):
        today = date.today()
        names = [
            f"srv_daily_review_list_{(today - timedelta(days=d)).strftime('%Y_%m_%d')}"
            for d in (20, 30, 40)
        ]
        session = self._make_session_with_partitions(names)
        agg = _make_aggregator(MagicMock())

        dropped = agg._drop_old_partitions(session, retention_days=14)

        assert dropped == 3

    def test_no_partitions_returns_zero(self):
        session = self._make_session_with_partitions([])
        agg = _make_aggregator(MagicMock())

        dropped = agg._drop_old_partitions(session, retention_days=14)

        assert dropped == 0

    def test_run_calls_drop_old_partitions(self, mock_session):
        agg = _make_aggregator(mock_session)
        for method in (
            "_upsert_fact_service_review_daily",
            "_upsert_fact_service_aspect_daily",
            "_upsert_fact_category_radar_scores",
            "_upsert_srv_daily_review_list",
        ):
            setattr(agg, method, MagicMock())
        agg._drop_old_partitions = MagicMock(return_value=2)

        result = agg.run(target_date=date(2025, 1, 15), retention_days=7)

        agg._drop_old_partitions.assert_called_once_with(mock_session, 7)
        assert result["dropped_partitions"] == 2
