"""Failure query helpers for existing pipeline state tables."""

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


def fetch_review_dead_letters(session, limit: int = 100):
    """Return review-level dead-letter-equivalent records."""
    return (
        session.query(ReviewMasterIndex)
        .filter(ReviewMasterIndex.processing_status == ProcessingStatusType.FAILED)
        .filter(ReviewMasterIndex.retry_count >= 3)
        .order_by(ReviewMasterIndex.review_created_at.asc(), ReviewMasterIndex.review_id.asc())
        .limit(limit)
        .all()
    )


def fetch_batch_dead_letters(session, limit: int = 100):
    """Return batch-level dead-letter records."""
    return (
        session.query(IngestionBatch)
        .filter(IngestionBatch.status == IngestionBatchStatusType.DEAD_LETTER)
        .order_by(IngestionBatch.updated_at.asc(), IngestionBatch.batch_id.asc())
        .limit(limit)
        .all()
    )
