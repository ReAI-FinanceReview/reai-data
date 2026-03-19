"""Unit tests for GoldOrchestrator."""

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

        assert result is True
        assert mock_rmi.processing_status == ProcessingStatusType.ANALYZED
        assert mock_rmi.error_message is None

    def test_embedding_failure_sets_failed(self, mock_session, mock_rmi):
        orch = _make_orchestrator()
        orch._embedding.process.return_value = False

        review_id = uuid4()
        mock_session.get.return_value = mock_rmi

        result = orch._process_one(mock_session, review_id)

        assert result is False
        assert mock_rmi.processing_status == ProcessingStatusType.FAILED
        assert mock_rmi.retry_count == 1

    def test_absa_failure_sets_failed(self, mock_session, mock_rmi):
        orch = _make_orchestrator()
        orch._embedding.process.return_value = True
        orch._absa.process.return_value = False

        review_id = uuid4()
        mock_session.get.return_value = mock_rmi

        result = orch._process_one(mock_session, review_id)

        assert result is False
        assert mock_rmi.processing_status == ProcessingStatusType.FAILED

    def test_action_failure_sets_failed(self, mock_session, mock_rmi):
        orch = _make_orchestrator()
        orch._embedding.process.return_value = True
        orch._absa.process.return_value = True
        orch._action.process.return_value = False

        review_id = uuid4()
        mock_session.get.return_value = mock_rmi

        result = orch._process_one(mock_session, review_id)

        assert result is False
        assert mock_rmi.processing_status == ProcessingStatusType.FAILED

    def test_exception_increments_retry_count(self, mock_session, mock_rmi):
        orch = _make_orchestrator()
        orch._embedding.process.side_effect = RuntimeError("network error")

        review_id = uuid4()
        mock_rmi.retry_count = 1
        mock_session.get.return_value = mock_rmi

        result = orch._process_one(mock_session, review_id)

        assert result is False
        assert mock_rmi.retry_count == 2

    def test_missing_rmi_record_logs_error(self, mock_session):
        orch = _make_orchestrator()
        orch._embedding.process.return_value = True
        orch._absa.process.return_value = True
        orch._action.process.return_value = True

        review_id = uuid4()
        mock_session.get.return_value = None

        result = orch._process_one(mock_session, review_id)
        assert result is True
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
        assert result == {"total": 0, "analyzed": 0, "failed": 0}
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
