"""Test Bronze Data Loading (Batch DLQ Architecture - Issue #19)

This module tests the crawl stage of the Batch DLQ architecture:
- Happy path: Parquet write + ingestion_batch PENDING registration
- Idempotency: Duplicate reviews are skipped
- Phase 1 failure: Parquet write fails → no ingestion_batch created
- Parquet file validation: schema, partitioning, data integrity
"""

import pytest
from pathlib import Path
from datetime import datetime, timezone
from unittest.mock import patch, MagicMock
from uuid6 import uuid7

from src.crawlers.appstore_crawler import AppStoreCrawler
from src.crawlers.exceptions import ParquetWriteError
from src.models.apps import App
from src.models.review_master_index import ReviewMasterIndex
from src.models.ingestion_batch import IngestionBatch
from src.models.enums import PlatformType, IngestionBatchStatusType
from src.utils.parquet_writer import read_parquet_to_schemas
from src.schemas.parquet.app_review import AppReviewSchema


def _make_crawler(test_db_session, temp_bronze_dir, enable_parquet=True):
    """Helper: create AppStoreCrawler with mocked dependencies."""
    from src.utils.parquet_writer import ParquetWriter

    with patch.object(AppStoreCrawler, '__init__', lambda self, *args, **kwargs: None):
        crawler = AppStoreCrawler()
        crawler.logger = MagicMock()
        crawler.enable_parquet = enable_parquet
        crawler.config = {}
        crawler.db_connector = MagicMock()
        crawler.db_connector.get_session.return_value = test_db_session

        if enable_parquet:
            crawler.parquet_writer = ParquetWriter(
                base_path=str(temp_bronze_dir),
                partition_by='year_month'
            )
        else:
            crawler.parquet_writer = None

    return crawler


# ========================================
# A. HAPPY PATH - CRAWL STAGE
# ========================================

@pytest.mark.requires_db
def test_crawl_saves_parquet_and_registers_pending_batch(
    test_db_session,
    temp_bronze_dir,
    sample_appstore_reviews
):
    """Test successful crawl: Parquet file created + ingestion_batch PENDING registered.

    Verifies:
    1. Parquet file created in year=2026/month=02/
    2. ingestion_batch record created with status=PENDING
    3. record_count matches number of reviews
    4. ReviewMasterIndex NOT created (deferred to load stage)
    5. App record created
    """
    crawler = _make_crawler(test_db_session, temp_bronze_dir)
    app_id = '123456789'
    app_name = 'Test App'

    batch_id, count, parquet_path = crawler.save_crawl_batch(
        app_id, app_name, sample_appstore_reviews, crawler._build_parquet_records
    )

    assert count == len(sample_appstore_reviews), f"Expected {len(sample_appstore_reviews)} records"
    assert batch_id is not None
    assert parquet_path is not None

    # Verify App created
    app = test_db_session.query(App).filter_by(
        platform_app_id=app_id,
        platform_type=PlatformType.APPSTORE
    ).first()
    assert app is not None

    # Verify ingestion_batch PENDING
    batch = test_db_session.query(IngestionBatch).filter_by(
        batch_id=batch_id
    ).first()
    assert batch is not None
    assert batch.status == IngestionBatchStatusType.PENDING
    assert batch.record_count == len(sample_appstore_reviews)
    assert batch.storage_path == str(parquet_path)

    # Verify Parquet file exists
    parquet_files = list(temp_bronze_dir.glob('**/*.parquet'))
    assert len(parquet_files) > 0

    # Verify NO ReviewMasterIndex created (deferred to load stage)
    review_count = test_db_session.query(ReviewMasterIndex).count()
    assert review_count == 0, "ReviewMasterIndex should NOT be created in crawl stage"


@pytest.mark.requires_db
def test_new_app_creation(test_db_session, temp_bronze_dir, sample_appstore_reviews):
    """Test that new App record is created when app doesn't exist."""
    crawler = _make_crawler(test_db_session, temp_bronze_dir)
    app_id = '999999999'

    app_before = test_db_session.query(App).filter_by(platform_app_id=app_id).first()
    assert app_before is None

    crawler.save_crawl_batch(app_id, 'Test App', sample_appstore_reviews, crawler._build_parquet_records)

    app_after = test_db_session.query(App).filter_by(
        platform_app_id=app_id,
        platform_type=PlatformType.APPSTORE
    ).first()
    assert app_after is not None
    assert app_after.platform_app_id == app_id


@pytest.mark.requires_db
def test_existing_app_reused(test_db_session, db_with_apps, temp_bronze_dir):
    """Test that existing App is reused instead of creating duplicate."""
    crawler = _make_crawler(test_db_session, temp_bronze_dir)
    app_id = '123456789'

    app_count_before = test_db_session.query(App).filter_by(
        platform_app_id=app_id
    ).count()
    assert app_count_before == 1

    new_reviews = [
        {
            'id': {'label': 'new_review_1'},
            'author': {'name': {'label': 'NewUser'}},
            'im:name': {'label': 'Test App'},
            'content': {'label': 'New review'},
            'im:rating': {'label': '5'},
            'updated': {'label': '2026-02-05T12:00:00Z'},
        }
    ]

    crawler.save_crawl_batch(app_id, 'Test App', new_reviews, crawler._build_parquet_records)

    app_count_after = test_db_session.query(App).filter_by(
        platform_app_id=app_id
    ).count()
    assert app_count_after == 1, "Should still have only 1 app"


# ========================================
# B. IDEMPOTENCY (DUPLICATE PREVENTION)
# ========================================

@pytest.mark.requires_db
def test_duplicate_reviews_skipped(
    test_db_session,
    temp_bronze_dir,
    sample_appstore_reviews
):
    """Test that duplicate reviews are skipped via platform_review_id."""
    crawler = _make_crawler(test_db_session, temp_bronze_dir)
    app_id = '123456789'

    # First crawl
    _, count1, _ = crawler.save_crawl_batch(
        app_id, 'Test App', sample_appstore_reviews, crawler._build_parquet_records
    )
    assert count1 == len(sample_appstore_reviews)

    # Second crawl (same data)
    _, count2, _ = crawler.save_crawl_batch(
        app_id, 'Test App', sample_appstore_reviews, crawler._build_parquet_records
    )
    assert count2 == 0, "Duplicate reviews should return 0"


@pytest.mark.requires_db
def test_partial_duplicate_batch(test_db_session, temp_bronze_dir):
    """Test handling of partially duplicated review batches (5 existing + 5 new)."""
    crawler = _make_crawler(test_db_session, temp_bronze_dir)
    app_id = '123456789'

    existing_reviews = [
        {
            'id': {'label': f'existing_{i}'},
            'author': {'name': {'label': f'User{i}'}},
            'im:name': {'label': 'Test App'},
            'content': {'label': f'Review {i}'},
            'im:rating': {'label': '5'},
            'updated': {'label': '2026-02-04T12:00:00Z'},
        }
        for i in range(5)
    ]

    _, count1, _ = crawler.save_crawl_batch(
        app_id, 'Test App', existing_reviews, crawler._build_parquet_records
    )
    assert count1 == 5

    mixed_reviews = existing_reviews + [
        {
            'id': {'label': f'new_{i}'},
            'author': {'name': {'label': f'NewUser{i}'}},
            'im:name': {'label': 'Test App'},
            'content': {'label': f'New review {i}'},
            'im:rating': {'label': '4'},
            'updated': {'label': '2026-02-05T12:00:00Z'},
        }
        for i in range(5)
    ]

    _, count2, _ = crawler.save_crawl_batch(
        app_id, 'Test App', mixed_reviews, crawler._build_parquet_records
    )
    assert count2 == 5, "Only 5 new reviews should be added"


@pytest.mark.requires_db
def test_empty_new_reviews_returns_zero(test_db_session, temp_bronze_dir):
    """Test that all-duplicate batch returns 0."""
    crawler = _make_crawler(test_db_session, temp_bronze_dir)
    app_id = '123456789'

    reviews = [
        {
            'id': {'label': 'test_review_1'},
            'author': {'name': {'label': 'User1'}},
            'im:name': {'label': 'Test App'},
            'content': {'label': 'Review 1'},
            'im:rating': {'label': '5'},
            'updated': {'label': '2026-02-04T12:00:00Z'},
        }
    ]

    _, count1, _ = crawler.save_crawl_batch(
        app_id, 'Test App', reviews, crawler._build_parquet_records
    )
    assert count1 == 1

    _, count2, _ = crawler.save_crawl_batch(
        app_id, 'Test App', reviews, crawler._build_parquet_records
    )
    assert count2 == 0


# ========================================
# C. PHASE 1 FAILURE - PARQUET WRITE FAILS
# ========================================

@pytest.mark.requires_db
def test_parquet_write_failure_raises_error(
    test_db_session,
    temp_bronze_dir,
    sample_appstore_reviews
):
    """Test that Parquet write failure raises ParquetWriteError."""
    crawler = _make_crawler(test_db_session, temp_bronze_dir)

    with patch.object(
        crawler.parquet_writer,
        'write_batch',
        side_effect=Exception("Disk full")
    ):
        with pytest.raises(ParquetWriteError) as exc_info:
            crawler.save_crawl_batch(
                '123456789', 'Test App', sample_appstore_reviews, crawler._build_parquet_records
            )
        assert "Disk full" in str(exc_info.value)


@pytest.mark.requires_db
def test_parquet_write_failure_no_ingestion_batch(
    test_db_session,
    temp_bronze_dir,
    sample_appstore_reviews
):
    """Test that Parquet failure prevents ingestion_batch creation.

    When Parquet write fails, no ingestion_batch record should be created.
    """
    crawler = _make_crawler(test_db_session, temp_bronze_dir)

    with patch.object(
        crawler.parquet_writer,
        'write_batch',
        side_effect=Exception("Permission denied")
    ):
        try:
            crawler.save_crawl_batch(
                '123456789', 'Test App', sample_appstore_reviews, crawler._build_parquet_records
            )
        except ParquetWriteError:
            pass

    # Verify NO ingestion_batch created
    batch_count = test_db_session.query(IngestionBatch).count()
    assert batch_count == 0, "ingestion_batch should NOT be created if Parquet write fails"

    # Verify NO Parquet files
    parquet_files = list(temp_bronze_dir.glob('**/*.parquet'))
    assert len(parquet_files) == 0


# ========================================
# G. PARQUET FILE VALIDATION
# ========================================

@pytest.mark.requires_db
def test_parquet_file_structure(test_db_session, temp_bronze_dir, sample_appstore_reviews):
    """Test that Parquet file matches AppReviewSchema."""
    crawler = _make_crawler(test_db_session, temp_bronze_dir)

    crawler.save_crawl_batch(
        '123456789', 'Test App', sample_appstore_reviews, crawler._build_parquet_records
    )

    parquet_file = list(temp_bronze_dir.glob('**/*.parquet'))[0]
    records = read_parquet_to_schemas(parquet_file, AppReviewSchema)

    assert all(hasattr(r, 'review_id') for r in records)
    assert all(hasattr(r, 'app_id') for r in records)
    assert all(hasattr(r, 'platform_review_id') for r in records)
    assert all(hasattr(r, 'review_text') for r in records)
    assert all(hasattr(r, 'rating') for r in records)


@pytest.mark.requires_db
def test_parquet_partitioning_year_month(
    test_db_session,
    temp_bronze_dir,
    sample_appstore_reviews
):
    """Test that Parquet uses year=YYYY/month=MM partitioning."""
    crawler = _make_crawler(test_db_session, temp_bronze_dir)

    crawler.save_crawl_batch(
        '123456789', 'Test App', sample_appstore_reviews, crawler._build_parquet_records
    )

    parquet_file = list(temp_bronze_dir.glob('**/*.parquet'))[0]
    assert 'year=' in str(parquet_file), "Should have year partition"
    assert 'month=' in str(parquet_file), "Should have month partition"


@pytest.mark.requires_db
def test_parquet_data_integrity(
    test_db_session,
    temp_bronze_dir,
    sample_appstore_reviews
):
    """Test that Parquet data matches input data."""
    crawler = _make_crawler(test_db_session, temp_bronze_dir)

    crawler.save_crawl_batch(
        '123456789', 'Test App', sample_appstore_reviews, crawler._build_parquet_records
    )

    parquet_file = list(temp_bronze_dir.glob('**/*.parquet'))[0]
    records = read_parquet_to_schemas(parquet_file, AppReviewSchema)

    assert len(records) == len(sample_appstore_reviews)
    for record in records:
        assert record.platform_type == 'APPSTORE'
        assert record.review_text is not None
        assert 1 <= record.rating <= 5


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
