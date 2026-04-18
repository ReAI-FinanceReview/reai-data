"""Unit tests for GoldAggregator."""

from datetime import date
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

    def test_run_defaults_to_today(self, mock_session):
        agg = _make_aggregator(mock_session)
        agg._upsert_fact_service_review_daily = MagicMock()
        agg._upsert_fact_service_aspect_daily = MagicMock()
        agg._upsert_fact_category_radar_scores = MagicMock()
        agg._upsert_srv_daily_review_list = MagicMock()

        result = agg.run(target_date=None)

        assert result["date"] == str(date.today())

    def test_run_commits_on_success(self, mock_session):
        agg = _make_aggregator(mock_session)
        for method in (
            "_upsert_fact_service_review_daily",
            "_upsert_fact_service_aspect_daily",
            "_upsert_fact_category_radar_scores",
            "_upsert_srv_daily_review_list",
        ):
            setattr(agg, method, MagicMock())

        agg.run(target_date=date.today())

        mock_session.commit.assert_called_once()
        mock_session.close.assert_called_once()

    def test_run_rolls_back_on_exception(self, mock_session):
        agg = _make_aggregator(mock_session)
        agg._upsert_fact_service_review_daily = MagicMock(side_effect=RuntimeError("db error"))

        with pytest.raises(RuntimeError):
            agg.run(target_date=date.today())

        mock_session.rollback.assert_called_once()
        mock_session.close.assert_called_once()
        mock_session.commit.assert_not_called()


# ---------------------------------------------------------------------------
# run_all() — #57 드레인 모드
# ---------------------------------------------------------------------------

class TestRunAll:
    def test_run_all_aggregates_all_analyzed_dates(self, mock_session):
        agg = _make_aggregator(mock_session)
        dates = [date(2025, 1, 13), date(2025, 1, 14), date(2025, 1, 15)]
        agg._fetch_analyzed_dates = MagicMock(return_value=dates)
        agg._upsert_fact_service_review_daily = MagicMock()
        agg._upsert_fact_service_aspect_daily = MagicMock()
        agg._upsert_fact_category_radar_scores = MagicMock()
        agg._upsert_srv_daily_review_list = MagicMock()

        result = agg.run_all()

        assert agg._upsert_fact_service_review_daily.call_count == 3
        assert agg._upsert_srv_daily_review_list.call_count == 3
        assert result["dates"] == ["2025-01-13", "2025-01-14", "2025-01-15"]
        assert "fact_service_review_daily" in result["tables_updated"]

    def test_run_all_empty_returns_early(self, mock_session):
        agg = _make_aggregator(mock_session)
        agg._fetch_analyzed_dates = MagicMock(return_value=[])

        result = agg.run_all()

        assert result["dates"] == []
        mock_session.commit.assert_not_called()

    def test_run_all_commits_once(self, mock_session):
        agg = _make_aggregator(mock_session)
        agg._fetch_analyzed_dates = MagicMock(return_value=[date(2025, 1, 15)])
        for method in (
            "_upsert_fact_service_review_daily",
            "_upsert_fact_service_aspect_daily",
            "_upsert_fact_category_radar_scores",
            "_upsert_srv_daily_review_list",
        ):
            setattr(agg, method, MagicMock())

        agg.run_all()

        mock_session.commit.assert_called_once()
        mock_session.close.assert_called_once()

    def test_run_all_rolls_back_on_exception(self, mock_session):
        agg = _make_aggregator(mock_session)
        agg._fetch_analyzed_dates = MagicMock(return_value=[date(2025, 1, 15)])
        agg._upsert_fact_service_review_daily = MagicMock(side_effect=RuntimeError("fail"))

        with pytest.raises(RuntimeError):
            agg.run_all()

        mock_session.rollback.assert_called_once()
        mock_session.commit.assert_not_called()


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
        assert "PARTITION OF srv_daily_review_list" in ddl_text

    def test_ensure_partition_uses_correct_date_range(self, mock_session):
        agg = _make_aggregator(mock_session)
        agg._ensure_partition(mock_session, date(2025, 1, 15))
        params = mock_session.execute.call_args[0][1]
        assert params["start"] == date(2025, 1, 15)
        assert params["end"] == date(2025, 1, 16)

    def test_ensure_partition_month_boundary(self, mock_session):
        agg = _make_aggregator(mock_session)
        agg._ensure_partition(mock_session, date(2025, 1, 31))
        params = mock_session.execute.call_args[0][1]
        assert params["end"] == date(2025, 2, 1)


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
