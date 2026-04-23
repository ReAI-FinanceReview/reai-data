"""Failure query helpers for existing pipeline state tables.

The failure-management policy for issue #7 uses existing durable state instead
of adding a separate row-level DLQ table. Batch-level terminal failures live in
``ingestion_batch`` as ``DEAD_LETTER`` rows, while review-level dead letters are
derived from ``review_master_index`` rows that remain ``FAILED`` after repeated
attempts. These helpers keep that policy in one place for operators, scripts,
and tests that need to inspect retry-exhausted work.
"""

from src.models.enums import IngestionBatchStatusType, ProcessingStatusType
from src.models.ingestion_batch import IngestionBatch
from src.models.review_master_index import ReviewMasterIndex


REVIEW_DEAD_LETTER_SQL = """
SELECT *
FROM review_master_index
WHERE processing_status = 'FAILED'
  AND retry_count >= 3
ORDER BY review_created_at ASC NULLS LAST, review_id
LIMIT :limit
"""

BATCH_DEAD_LETTER_SQL = """
SELECT *
FROM ingestion_batch
WHERE status = 'DEAD_LETTER'
ORDER BY updated_at ASC, batch_id
LIMIT :limit
"""


def _normalize_limit(limit: int) -> int:
    """Return a safe positive limit value for operational query helpers."""
    if not isinstance(limit, int) or limit <= 0:
        return 100
    return limit


def fetch_review_dead_letters(session, limit: int = 100):
    """Return retry-exhausted review rows from ``review_master_index``.

    A review is treated as dead-letter-equivalent when it is still in FAILED
    state after at least three attempts. The caller owns any follow-up action,
    such as manual inspection, retry scheduling, or reporting.
    """
    limit = _normalize_limit(limit)
    return (
        session.query(ReviewMasterIndex)
        .filter(ReviewMasterIndex.processing_status == ProcessingStatusType.FAILED)
        .filter(ReviewMasterIndex.retry_count >= 3)
        .order_by(
            ReviewMasterIndex.review_created_at.asc().nulls_last(),
            ReviewMasterIndex.review_id.asc(),
        )
        .limit(limit)
        .all()
    )


def fetch_batch_dead_letters(session, limit: int = 100):
    """Return terminal batch failures from ``ingestion_batch``.

    Batch dead letters represent ingestion jobs that have already been promoted
    to ``DEAD_LETTER`` status by batch-level retry policy.
    """
    limit = _normalize_limit(limit)
    return (
        session.query(IngestionBatch)
        .filter(IngestionBatch.status == IngestionBatchStatusType.DEAD_LETTER)
        .order_by(IngestionBatch.updated_at.asc(), IngestionBatch.batch_id.asc())
        .limit(limit)
        .all()
    )
