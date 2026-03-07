"""Test BatchLoader (Load Stage - Issue #19)

This module tests the load stage of the Batch DLQ architecture:
- Happy path: PENDING batch → LOADED, ReviewMasterIndex status=RAW
- Idempotency: already-loaded reviews are skipped
- Failure handling: DB error → FAILED status + retry_count increment
- Max retries: retry_count >= max_retries → DEAD_LETTER
- Idempotency for loaded batches: LOADED batch is not reprocessed
- Missing Parquet: file not found → FAILED
- ReviewMasterIndex field validation after load
"""

import pytest
from pathlib import Path
from datetime import datetime, timezone
from unittest.mock import patch, MagicMock
from uuid6 import uuid7

from src.loaders.batch_loader import BatchLoader
from src.models.apps import App
from src.models.review_master_index import ReviewMasterIndex
from src.models.ingestion_batch import IngestionBatch
from src.models.enums import PlatformType, ProcessingStatusType, IngestionBatchStatusType
from src.schemas.parquet.app_review import AppReviewSchema


def _make_loader(test_db_session):
    """Helper: create BatchLoader with mocked DB connector."""
    loader = BatchLoader.__new__(BatchLoader)
    loader.logger = MagicMock()
    loader.db_connector = MagicMock()
    loader.db_connector.get_session.return_value = test_db_session
    return loader


# ========================================
# A. HAPPY PATH
# ========================================

@pytest.mark.requires_db
def test_load_pending_batch_success(
    test_db_session,
    db_with_pending_batches,
    temp_bronze_dir
):
    """Test PENDING batch → LOADED, ReviewMasterIndex created with status=RAW."""
    loader = _make_loader(test_db_session)

    loaded = loader.load_pending_batches(limit=100)

    assert loaded >= 1, "At least 1 batch should be loaded"

    # Verify batch status changed to LOADED
    batch = test_db_session.query(IngestionBatch).filter_by(
        status=IngestionBatchStatusType.LOADED
    ).first()
    assert batch is not None
    assert batch.loaded_at is not None

    # Verify ReviewMasterIndex records created with RAW status
    reviews = test_db_session.query(ReviewMasterIndex).all()
    assert len(reviews) > 0

    for review in reviews:
        assert review.processing_status == ProcessingStatusType.RAW
        assert review.error_message is None
        assert review.retry_count == 0
        assert review.is_active is True


@pytest.mark.requires_db
def test_load_dedup_skips_existing(
    test_db_session,
    db_with_pending_batches,
    temp_bronze_dir
):
    """Test that already-indexed reviews are skipped (idempotency)."""
    loader = _make_loader(test_db_session)

    # First load
    loader.load_pending_batches(limit=100)
    count_after_first = test_db_session.query(ReviewMasterIndex).count()

    # Create another batch with same data — simulate re-load scenario
    # by manually re-queuing the loaded batch
    batch = test_db_session.query(IngestionBatch).filter_by(
        status=IngestionBatchStatusType.LOADED
    ).first()
    assert batch is not None

    # Re-set batch to PENDING and call again
    batch.status = IngestionBatchStatusType.PENDING
    test_db_session.commit()

    loader.load_pending_batches(limit=100)
    count_after_second = test_db_session.query(ReviewMasterIndex).count()

    assert count_after_second == count_after_first, \
        "No new ReviewMasterIndex records should be created (idempotent)"


@pytest.mark.requires_db
def test_load_missing_parquet_marks_failed(test_db_session, temp_bronze_dir):
    """Test that missing Parquet file causes FAILED status."""
    now = datetime.now(timezone.utc)
    batch = IngestionBatch(
        batch_id=uuid7(),
        source_type=PlatformType.APPSTORE,
        platform_app_id='123456789',
        app_name='Test App',
        storage_path='/tmp/nonexistent_file.parquet',
        file_format='parquet',
        record_count=5,
        status=IngestionBatchStatusType.PENDING,
        retry_count=0,
        max_retries=3,
        created_at=now,
        updated_at=now
    )
    test_db_session.add(batch)
    test_db_session.commit()

    loader = _make_loader(test_db_session)
    loader.load_pending_batches(limit=100)

    # Verify batch is now FAILED
    updated_batch = test_db_session.query(IngestionBatch).filter_by(
        batch_id=batch.batch_id
    ).first()
    assert updated_batch.status == IngestionBatchStatusType.FAILED
    assert updated_batch.retry_count == 1
    assert updated_batch.error_message is not None


@pytest.mark.requires_db
def test_load_db_failure_marks_batch_failed(test_db_session, db_with_pending_batches):
    """Test DB failure causes FAILED status + retry_count increment."""
    loader = _make_loader(test_db_session)

    with patch.object(loader, '_load_single_batch', side_effect=Exception("DB connection lost")):
        loader.load_pending_batches(limit=100)

    batch = test_db_session.query(IngestionBatch).filter_by(
        status=IngestionBatchStatusType.FAILED
    ).first()
    assert batch is not None
    assert batch.retry_count == 1
    assert "DB connection lost" in (batch.error_message or "")


@pytest.mark.requires_db
def test_load_max_retries_dead_letter(test_db_session, temp_bronze_dir):
    """Test retry_count >= max_retries causes DEAD_LETTER status."""
    now = datetime.now(timezone.utc)
    batch = IngestionBatch(
        batch_id=uuid7(),
        source_type=PlatformType.APPSTORE,
        platform_app_id='123456789',
        app_name='Test App',
        storage_path='/tmp/nonexistent_dlq.parquet',
        file_format='parquet',
        record_count=2,
        status=IngestionBatchStatusType.FAILED,
        retry_count=2,  # One more failure → 3 = max_retries → DEAD_LETTER
        max_retries=3,
        created_at=now,
        updated_at=now
    )
    test_db_session.add(batch)
    test_db_session.commit()

    loader = _make_loader(test_db_session)
    loader.load_pending_batches(limit=100)

    updated_batch = test_db_session.query(IngestionBatch).filter_by(
        batch_id=batch.batch_id
    ).first()
    assert updated_batch.status == IngestionBatchStatusType.DEAD_LETTER
    assert updated_batch.retry_count == 3


@pytest.mark.requires_db
def test_load_idempotency_loaded_batch_skipped(test_db_session, temp_bronze_dir):
    """Test that LOADED batches are not reprocessed."""
    now = datetime.now(timezone.utc)
    batch = IngestionBatch(
        batch_id=uuid7(),
        source_type=PlatformType.APPSTORE,
        platform_app_id='123456789',
        app_name='Test App',
        storage_path='/tmp/already_loaded.parquet',
        file_format='parquet',
        record_count=3,
        status=IngestionBatchStatusType.LOADED,  # Already loaded
        retry_count=0,
        max_retries=3,
        loaded_at=now,
        created_at=now,
        updated_at=now
    )
    test_db_session.add(batch)
    test_db_session.commit()

    loader = _make_loader(test_db_session)
    result = loader.load_pending_batches(limit=100)

    # LOADED batch should be ignored (0 batches processed)
    assert result == 0, "LOADED batches should not be reprocessed"


# ========================================
# B. REVIEW MASTER INDEX FIELD VALIDATION
# ========================================

@pytest.mark.requires_db
def test_review_master_index_fields_after_load(
    test_db_session,
    db_with_pending_batches,
    temp_bronze_dir
):
    """Test that ReviewMasterIndex fields are correctly populated after load."""
    loader = _make_loader(test_db_session)
    loader.load_pending_batches(limit=100)

    reviews = test_db_session.query(ReviewMasterIndex).all()
    assert len(reviews) > 0

    for review in reviews:
        assert review.review_id is not None
        assert review.app_id is not None
        assert review.platform_review_id is not None
        assert review.platform_type == PlatformType.APPSTORE
        assert review.review_created_at is not None
        assert review.ingested_at is not None
        assert review.processing_status == ProcessingStatusType.RAW
        assert review.parquet_written_at is not None
        assert review.storage_path is not None
        assert review.is_active is True
        assert review.is_reply is False
        assert review.error_message is None
        assert review.retry_count == 0


@pytest.mark.requires_db
def test_review_id_is_uuid_v7(test_db_session, db_with_pending_batches, temp_bronze_dir):
    """Test that review_id in ReviewMasterIndex is UUID v7."""
    loader = _make_loader(test_db_session)
    loader.load_pending_batches(limit=100)

    reviews = test_db_session.query(ReviewMasterIndex).all()
    assert len(reviews) > 0

    for review in reviews:
        uuid_str = str(review.review_id)
        version = int(uuid_str[14], 16)
        assert version == 7, f"review_id should be UUID v7, got version {version}"


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
