from unittest.mock import MagicMock, sentinel

from src.models.ingestion_batch import IngestionBatch
from src.models.review_master_index import ReviewMasterIndex
from src.pipeline.failures import (
    BATCH_DEAD_LETTER_SQL,
    REVIEW_DEAD_LETTER_SQL,
    fetch_batch_dead_letters,
    fetch_review_dead_letters,
)


def _session_with_query_result(result):
    session = MagicMock()
    query = MagicMock()
    query.filter.return_value = query
    query.order_by.return_value = query
    query.limit.return_value = query
    query.all.return_value = result
    session.query.return_value = query
    return session, query


def test_fetch_review_dead_letters_queries_failed_max_retry_reviews():
    session, query = _session_with_query_result([sentinel.review])

    results = fetch_review_dead_letters(session, limit=25)

    assert results == [sentinel.review]
    session.query.assert_called_once_with(ReviewMasterIndex)
    assert query.filter.call_count == 2
    query.limit.assert_called_once_with(25)
    assert "processing_status = 'FAILED'" in REVIEW_DEAD_LETTER_SQL
    assert "retry_count >= 3" in REVIEW_DEAD_LETTER_SQL


def test_fetch_batch_dead_letters_queries_dead_letter_batches():
    session, query = _session_with_query_result([sentinel.batch])

    results = fetch_batch_dead_letters(session, limit=10)

    assert results == [sentinel.batch]
    session.query.assert_called_once_with(IngestionBatch)
    query.filter.assert_called_once()
    query.limit.assert_called_once_with(10)
    assert "status = 'DEAD_LETTER'" in BATCH_DEAD_LETTER_SQL
