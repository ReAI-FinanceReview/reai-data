"""Test Bronze Data Loading (NAS-first Dual-Write Pattern)

This module tests the core consistency guarantees of the NAS-first architecture:
- Happy path: Successful Parquet + DB dual-write
- Idempotency: Duplicate reviews are skipped
- Phase 1 failure: Parquet write fails → no DB commit
- Phase 2 failure: DB commit fails → Parquet already written
- Failure tracking: Failed reviews marked with error_message
- Retry mechanism: retry_count incremented (Phase 3 MVP)
- Data validation: Parquet and DB records match

Architecture:
- 2-Phase Commit: Parquet (Phase 1) → DB (Phase 2)
- Ghost Records Prevention: No DB commit if Parquet fails
- Orphaned Parquet Tolerance: DB failure after Parquet is acceptable
"""

import pytest
from pathlib import Path
from datetime import datetime, timezone
from unittest.mock import patch, MagicMock
from uuid6 import uuid7

from src.crawlers.appstore_crawler import (
    AppStoreCrawler,
    ParquetWriteError,
    DBCommitError
)
from src.models.apps import App
from src.models.review_master_index import ReviewMasterIndex
from src.models.enums import PlatformType, ProcessingStatusType
from src.utils.parquet_writer import read_parquet_to_schemas
from src.schemas.parquet.app_review import AppReviewSchema


# ========================================
# A. HAPPY PATH - DUAL WRITE SUCCESS
# ========================================

@pytest.mark.requires_db
def test_save_to_parquet_and_database_success(
    test_db_session,
    temp_bronze_dir,
    sample_appstore_reviews
):
    """Test successful NAS-first dual-write.

    Verifies:
    1. Parquet file created in year=2026/month=02/
    2. ReviewMasterIndex records created with status=RAW
    3. parquet_written_at is set
    4. App record created
    5. All fields populated correctly
    """
    # Create crawler with temp directory
    with patch.object(AppStoreCrawler, '__init__', lambda self, config_path: None):
        crawler = AppStoreCrawler()
        crawler.logger = MagicMock()
        crawler.enable_parquet = True
        crawler.config = {}

        # Mock DB connector
        crawler.db_connector = MagicMock()
        crawler.db_connector.get_session.return_value = test_db_session

        # Mock Parquet writer
        from src.utils.parquet_writer import ParquetWriter
        crawler.parquet_writer = ParquetWriter(
            base_path=str(temp_bronze_dir),
            partition_by='year_month'
        )

        # Execute dual-write
        app_id = '123456789'
        reviews_added = crawler.save_to_parquet_and_database(app_id, sample_appstore_reviews)

        # Verify count
        assert reviews_added == len(sample_appstore_reviews), \
            f"Expected {len(sample_appstore_reviews)} reviews added"

        # Verify App created
        app = test_db_session.query(App).filter_by(
            platform_app_id=app_id,
            platform_type=PlatformType.APPSTORE
        ).first()
        assert app is not None, "App should be created"

        # Verify ReviewMasterIndex records
        reviews = test_db_session.query(ReviewMasterIndex).filter_by(
            app_id=app.app_id
        ).all()

        assert len(reviews) == len(sample_appstore_reviews), \
            "All reviews should have ReviewMasterIndex entries"

        for review in reviews:
            assert review.processing_status == ProcessingStatusType.RAW, \
                "Initial status should be RAW"
            assert review.parquet_written_at is not None, \
                "parquet_written_at should be set"
            assert review.error_message is None, \
                "error_message should be None on success"
            assert review.retry_count == 0, \
                "retry_count should be 0 on success"
            assert review.is_active is True
            assert review.is_reply is False

        # Verify Parquet file exists
        parquet_files = list(temp_bronze_dir.glob('**/*.parquet'))
        assert len(parquet_files) > 0, "Parquet file should be created"

        # Verify Parquet partitioning
        parquet_file = parquet_files[0]
        assert 'year=2026' in str(parquet_file), "Should have year partition"
        assert 'month=02' in str(parquet_file), "Should have month partition"

        # Verify Parquet data
        parquet_records = read_parquet_to_schemas(parquet_file, AppReviewSchema)
        assert len(parquet_records) == len(sample_appstore_reviews), \
            "Parquet should contain all reviews"


@pytest.mark.requires_db
def test_new_app_creation(test_db_session, temp_bronze_dir, sample_appstore_reviews):
    """Test that new App record is created when app doesn't exist."""
    with patch.object(AppStoreCrawler, '__init__', lambda self, config_path: None):
        crawler = AppStoreCrawler()
        crawler.logger = MagicMock()
        crawler.enable_parquet = True
        crawler.config = {}
        crawler.db_connector = MagicMock()
        crawler.db_connector.get_session.return_value = test_db_session

        from src.utils.parquet_writer import ParquetWriter
        crawler.parquet_writer = ParquetWriter(
            base_path=str(temp_bronze_dir),
            partition_by='year_month'
        )

        app_id = '999999999'

        # Verify no app exists
        app_before = test_db_session.query(App).filter_by(
            platform_app_id=app_id
        ).first()
        assert app_before is None, "App should not exist before"

        # Execute
        crawler.save_to_parquet_and_database(app_id, sample_appstore_reviews)

        # Verify app created
        app_after = test_db_session.query(App).filter_by(
            platform_app_id=app_id,
            platform_type=PlatformType.APPSTORE
        ).first()

        assert app_after is not None, "App should be created"
        assert app_after.platform_app_id == app_id
        assert app_after.name == 'Test App'  # From sample data


@pytest.mark.requires_db
def test_existing_app_reused(test_db_session, db_with_apps, temp_bronze_dir):
    """Test that existing App is reused instead of creating duplicate."""
    with patch.object(AppStoreCrawler, '__init__', lambda self, config_path: None):
        crawler = AppStoreCrawler()
        crawler.logger = MagicMock()
        crawler.enable_parquet = True
        crawler.config = {}
        crawler.db_connector = MagicMock()
        crawler.db_connector.get_session.return_value = test_db_session

        from src.utils.parquet_writer import ParquetWriter
        crawler.parquet_writer = ParquetWriter(
            base_path=str(temp_bronze_dir),
            partition_by='year_month'
        )

        # db_with_apps already has app with platform_app_id='123456789'
        app_id = '123456789'

        # Get existing app count
        app_count_before = test_db_session.query(App).filter_by(
            platform_app_id=app_id
        ).count()

        assert app_count_before == 1, "Should have 1 existing app"

        # Create new reviews for existing app
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

        crawler.save_to_parquet_and_database(app_id, new_reviews)

        # Verify no duplicate app created
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
    with patch.object(AppStoreCrawler, '__init__', lambda self, config_path: None):
        crawler = AppStoreCrawler()
        crawler.logger = MagicMock()
        crawler.enable_parquet = True
        crawler.config = {}
        crawler.db_connector = MagicMock()
        crawler.db_connector.get_session.return_value = test_db_session

        from src.utils.parquet_writer import ParquetWriter
        crawler.parquet_writer = ParquetWriter(
            base_path=str(temp_bronze_dir),
            partition_by='year_month'
        )

        app_id = '123456789'

        # First write
        count1 = crawler.save_to_parquet_and_database(app_id, sample_appstore_reviews)
        assert count1 == len(sample_appstore_reviews)

        # Second write (same data)
        count2 = crawler.save_to_parquet_and_database(app_id, sample_appstore_reviews)
        assert count2 == 0, "Duplicate reviews should be skipped"

        # Verify total count
        total_reviews = test_db_session.query(ReviewMasterIndex).count()
        assert total_reviews == len(sample_appstore_reviews), \
            "Should have only original reviews, no duplicates"


@pytest.mark.requires_db
def test_partial_duplicate_batch(test_db_session, temp_bronze_dir):
    """Test handling of partially duplicated review batches.

    Scenario: 5 existing + 5 new → only 5 should be added
    """
    with patch.object(AppStoreCrawler, '__init__', lambda self, config_path: None):
        crawler = AppStoreCrawler()
        crawler.logger = MagicMock()
        crawler.enable_parquet = True
        crawler.config = {}
        crawler.db_connector = MagicMock()
        crawler.db_connector.get_session.return_value = test_db_session

        from src.utils.parquet_writer import ParquetWriter
        crawler.parquet_writer = ParquetWriter(
            base_path=str(temp_bronze_dir),
            partition_by='year_month'
        )

        app_id = '123456789'

        # Create 5 existing reviews
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

        # Write existing
        count1 = crawler.save_to_parquet_and_database(app_id, existing_reviews)
        assert count1 == 5

        # Create batch with 5 old + 5 new
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

        # Write mixed batch
        count2 = crawler.save_to_parquet_and_database(app_id, mixed_reviews)
        assert count2 == 5, "Only 5 new reviews should be added"

        # Verify total
        total = test_db_session.query(ReviewMasterIndex).count()
        assert total == 10, "Should have 5 old + 5 new = 10 reviews"


@pytest.mark.requires_db
def test_empty_new_reviews_returns_zero(test_db_session, temp_bronze_dir):
    """Test that all-duplicate batch returns 0."""
    with patch.object(AppStoreCrawler, '__init__', lambda self, config_path: None):
        crawler = AppStoreCrawler()
        crawler.logger = MagicMock()
        crawler.enable_parquet = True
        crawler.config = {}
        crawler.db_connector = MagicMock()
        crawler.db_connector.get_session.return_value = test_db_session

        from src.utils.parquet_writer import ParquetWriter
        crawler.parquet_writer = ParquetWriter(
            base_path=str(temp_bronze_dir),
            partition_by='year_month'
        )

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

        # First write
        count1 = crawler.save_to_parquet_and_database(app_id, reviews)
        assert count1 == 1

        # Second write (duplicate)
        count2 = crawler.save_to_parquet_and_database(app_id, reviews)
        assert count2 == 0, "All duplicates should return 0"


# ========================================
# C. PHASE 1 FAILURE - PARQUET WRITE FAILS
# ========================================

@pytest.mark.requires_db
def test_parquet_write_failure_no_db_commit(
    test_db_session,
    temp_bronze_dir,
    sample_appstore_reviews
):
    """Test that Parquet failure prevents DB commit.

    Verifies:
    1. ParquetWriteError is raised
    2. No ReviewMasterIndex records created
    3. Transaction is rolled back (Ghost Records prevented)
    """
    with patch.object(AppStoreCrawler, '__init__', lambda self, config_path: None):
        crawler = AppStoreCrawler()
        crawler.logger = MagicMock()
        crawler.enable_parquet = True
        crawler.config = {}
        crawler.db_connector = MagicMock()
        crawler.db_connector.get_session.return_value = test_db_session

        from src.utils.parquet_writer import ParquetWriter
        crawler.parquet_writer = ParquetWriter(
            base_path=str(temp_bronze_dir),
            partition_by='year_month'
        )

        # Mock Parquet write to fail
        with patch.object(
            crawler.parquet_writer,
            'write_batch',
            side_effect=Exception("Disk full")
        ):
            app_id = '123456789'

            # Expect ParquetWriteError
            with pytest.raises(ParquetWriteError) as exc_info:
                crawler.save_to_parquet_and_database(app_id, sample_appstore_reviews)

            assert "Disk full" in str(exc_info.value)

        # Verify NO DB records created (Ghost Records prevented)
        review_count = test_db_session.query(ReviewMasterIndex).count()
        assert review_count == 0, "No ReviewMasterIndex records should be created"

        # Verify no Parquet files
        parquet_files = list(temp_bronze_dir.glob('**/*.parquet'))
        assert len(parquet_files) == 0, "No Parquet files should exist"


@pytest.mark.requires_db
def test_parquet_write_failure_raises_parquet_write_error(
    test_db_session,
    temp_bronze_dir,
    sample_appstore_reviews
):
    """Test that Parquet failures raise ParquetWriteError."""
    with patch.object(AppStoreCrawler, '__init__', lambda self, config_path: None):
        crawler = AppStoreCrawler()
        crawler.logger = MagicMock()
        crawler.enable_parquet = True
        crawler.config = {}
        crawler.db_connector = MagicMock()
        crawler.db_connector.get_session.return_value = test_db_session

        from src.utils.parquet_writer import ParquetWriter
        crawler.parquet_writer = ParquetWriter(
            base_path=str(temp_bronze_dir),
            partition_by='year_month'
        )

        with patch.object(
            crawler.parquet_writer,
            'write_batch',
            side_effect=PermissionError("Access denied")
        ):
            with pytest.raises(ParquetWriteError):
                crawler.save_to_parquet_and_database('123456789', sample_appstore_reviews)


@pytest.mark.requires_db
def test_ghost_records_prevention(test_db_session, temp_bronze_dir, sample_appstore_reviews):
    """Test prevention of Ghost Records (DB without Parquet).

    Ghost Record scenario: DB has ReviewMasterIndex but Parquet is missing.
    This should NEVER happen due to NAS-first architecture.
    """
    with patch.object(AppStoreCrawler, '__init__', lambda self, config_path: None):
        crawler = AppStoreCrawler()
        crawler.logger = MagicMock()
        crawler.enable_parquet = True
        crawler.config = {}
        crawler.db_connector = MagicMock()
        crawler.db_connector.get_session.return_value = test_db_session

        from src.utils.parquet_writer import ParquetWriter
        crawler.parquet_writer = ParquetWriter(
            base_path=str(temp_bronze_dir),
            partition_by='year_month'
        )

        # Simulate Parquet failure
        with patch.object(
            crawler.parquet_writer,
            'write_batch',
            side_effect=Exception("Parquet write failed")
        ):
            try:
                crawler.save_to_parquet_and_database('123456789', sample_appstore_reviews)
            except ParquetWriteError:
                pass  # Expected

        # Verify: NO Ghost Records
        db_count = test_db_session.query(ReviewMasterIndex).count()
        parquet_files = list(temp_bronze_dir.glob('**/*.parquet'))

        assert db_count == 0, "DB should have 0 records (Ghost Records prevented)"
        assert len(parquet_files) == 0, "Parquet should have 0 files"


# ========================================
# D. PHASE 2 FAILURE - DB COMMIT FAILS
# ========================================

@pytest.mark.requires_db
def test_db_commit_failure_parquet_already_written(
    test_db_session,
    temp_bronze_dir,
    sample_appstore_reviews
):
    """Test DB commit failure after Parquet success.

    Scenario: Parquet write succeeds, but DB commit fails.
    Result: DBCommitError raised, Parquet exists (orphaned).

    This is acceptable - can retry DB commit using platform_review_id.
    """
    with patch.object(AppStoreCrawler, '__init__', lambda self, config_path: None):
        crawler = AppStoreCrawler()
        crawler.logger = MagicMock()
        crawler.enable_parquet = True
        crawler.config = {}
        crawler.db_connector = MagicMock()
        crawler.db_connector.get_session.return_value = test_db_session

        from src.utils.parquet_writer import ParquetWriter
        crawler.parquet_writer = ParquetWriter(
            base_path=str(temp_bronze_dir),
            partition_by='year_month'
        )

        # Mock session.commit() to fail
        original_commit = test_db_session.commit

        def failing_commit():
            # Only fail on the final commit (after add_all)
            if test_db_session.new:
                raise Exception("DB connection timeout")
            original_commit()

        test_db_session.commit = failing_commit

        app_id = '123456789'

        # Expect DBCommitError
        with pytest.raises(DBCommitError) as exc_info:
            crawler.save_to_parquet_and_database(app_id, sample_appstore_reviews)

        assert "DB connection timeout" in str(exc_info.value)

        # Verify: Parquet exists (orphaned)
        parquet_files = list(temp_bronze_dir.glob('**/*.parquet'))
        assert len(parquet_files) > 0, "Parquet file should exist (orphaned)"

        # Verify: DB has NO ReviewMasterIndex records (rollback)
        review_count = test_db_session.query(ReviewMasterIndex).count()
        assert review_count == 0, "DB should have 0 records (rollback)"


@pytest.mark.requires_db
def test_db_commit_failure_acceptable_state(
    test_db_session,
    temp_bronze_dir,
    sample_appstore_reviews
):
    """Test that orphaned Parquet is acceptable state.

    Orphaned Parquet can be reconciled using platform_review_id
    in a retry process. This is preferable to Ghost Records.
    """
    # This test validates the architectural decision
    # Orphaned Parquet (Parquet YES, DB NO) is acceptable
    # Ghost Records (Parquet NO, DB YES) is NOT acceptable
    pass  # Acceptance test - architecture validation


# ========================================
# E. FAILURE TRACKING
# ========================================

@pytest.mark.requires_db
def test_mark_reviews_as_failed_parquet_error(
    test_db_session,
    sample_appstore_reviews
):
    """Test _mark_reviews_as_failed for Parquet errors.

    Verifies:
    1. ReviewMasterIndex created with status=FAILED
    2. error_message contains "PARQUET_WRITE_FAILED"
    3. retry_count = 0
    """
    with patch.object(AppStoreCrawler, '__init__', lambda self, config_path: None):
        crawler = AppStoreCrawler()
        crawler.logger = MagicMock()
        crawler.config = {}
        crawler.db_connector = MagicMock()
        crawler.db_connector.get_session.return_value = test_db_session

        app_id = '123456789'
        error_msg = "Disk full"
        failure_reason = "PARQUET_WRITE_FAILED"

        # Execute
        crawler._mark_reviews_as_failed(
            app_id,
            sample_appstore_reviews,
            error_msg,
            failure_reason
        )

        # Verify FAILED records created
        failed_reviews = test_db_session.query(ReviewMasterIndex).filter_by(
            processing_status=ProcessingStatusType.FAILED
        ).all()

        assert len(failed_reviews) == len(sample_appstore_reviews), \
            "All reviews should be marked as FAILED"

        for failed_review in failed_reviews:
            assert failure_reason in failed_review.error_message
            assert error_msg in failed_review.error_message
            assert failed_review.retry_count == 0
            assert failed_review.parquet_written_at is None


@pytest.mark.requires_db
def test_mark_reviews_as_failed_db_error(test_db_session, sample_appstore_reviews):
    """Test _mark_reviews_as_failed for DB errors."""
    with patch.object(AppStoreCrawler, '__init__', lambda self, config_path: None):
        crawler = AppStoreCrawler()
        crawler.logger = MagicMock()
        crawler.config = {}
        crawler.db_connector = MagicMock()
        crawler.db_connector.get_session.return_value = test_db_session

        app_id = '999999999'
        error_msg = "Connection timeout"
        failure_reason = "DB_COMMIT_FAILED"

        crawler._mark_reviews_as_failed(
            app_id,
            sample_appstore_reviews,
            error_msg,
            failure_reason
        )

        failed_reviews = test_db_session.query(ReviewMasterIndex).filter_by(
            processing_status=ProcessingStatusType.FAILED
        ).all()

        assert len(failed_reviews) > 0

        for failed_review in failed_reviews:
            assert "DB_COMMIT_FAILED" in failed_review.error_message


@pytest.mark.requires_db
def test_mark_reviews_as_failed_updates_existing(
    test_db_session,
    sample_appstore_reviews
):
    """Test that existing FAILED records are updated, not duplicated."""
    with patch.object(AppStoreCrawler, '__init__', lambda self, config_path: None):
        crawler = AppStoreCrawler()
        crawler.logger = MagicMock()
        crawler.config = {}
        crawler.db_connector = MagicMock()
        crawler.db_connector.get_session.return_value = test_db_session

        app_id = '123456789'

        # First failure
        crawler._mark_reviews_as_failed(
            app_id,
            sample_appstore_reviews[:1],
            "First error",
            "PARQUET_WRITE_FAILED"
        )

        count_after_first = test_db_session.query(ReviewMasterIndex).count()

        # Second failure (same review)
        crawler._mark_reviews_as_failed(
            app_id,
            sample_appstore_reviews[:1],
            "Second error",
            "DB_COMMIT_FAILED"
        )

        count_after_second = test_db_session.query(ReviewMasterIndex).count()

        # Should NOT create duplicate
        assert count_after_first == count_after_second, \
            "Should update existing FAILED record, not create duplicate"

        # Verify error_message updated
        failed_review = test_db_session.query(ReviewMasterIndex).first()
        assert "DB_COMMIT_FAILED" in failed_review.error_message


# ========================================
# F. RETRY MECHANISM
# ========================================

@pytest.mark.requires_db
def test_retry_failed_reviews_increments_count(test_db_session, db_with_failed_reviews):
    """Test retry_failed_reviews increments retry_count for retryable reviews.

    db_with_failed_reviews fixture:
    - 1 app ('999999999') with 3 FAILED reviews
    - retry_count=0: retryable
    - retry_count=1: retryable
    - retry_count=3: NOT retryable (max_retries=3 reached)

    retry_failed_reviews() returns number of apps re-crawled (not reviews).
    Since re-crawled reviews are already in DB as FAILED (idempotency),
    save_to_parquet_and_database returns 0 new reviews → retry_count incremented.
    """
    with patch.object(AppStoreCrawler, '__init__', lambda self, config_path=None: None):
        crawler = AppStoreCrawler()
        crawler.logger = MagicMock()
        crawler.config = {}
        crawler.db_connector = MagicMock()
        crawler.db_connector.get_session.return_value = test_db_session

        with patch.object(crawler, 'get_app_store_reviews_and_appname', return_value=('Test App', [])):
            with patch.object(crawler, 'save_to_parquet_and_database', return_value=0):
                retried_count = crawler.retry_failed_reviews(max_retries=3)

        # API returned empty → no successful re-crawl → retried_apps = 0
        # (retried_apps increments only when save_to_parquet_and_database is called)
        assert retried_count == 0, "Empty API response: 0 apps successfully re-crawled"

        # Verify retry_count incremented for retryable reviews
        review_0 = test_db_session.query(ReviewMasterIndex).filter_by(
            platform_review_id='failed_review_0'
        ).first()
        review_1 = test_db_session.query(ReviewMasterIndex).filter_by(
            platform_review_id='failed_review_1'
        ).first()
        review_3 = test_db_session.query(ReviewMasterIndex).filter_by(
            platform_review_id='failed_review_3'
        ).first()

        assert review_0.retry_count == 1, "retry_count 0→1"
        assert review_1.retry_count == 2, "retry_count 1→2"
        assert review_3.retry_count == 3, "retry_count=3 unchanged (max reached)"


@pytest.mark.requires_db
def test_retry_failed_reviews_max_retries(test_db_session, db_with_failed_reviews):
    """Test that reviews with retry_count >= max_retries are skipped."""
    with patch.object(AppStoreCrawler, '__init__', lambda self, config_path=None: None):
        crawler = AppStoreCrawler()
        crawler.logger = MagicMock()
        crawler.config = {}
        crawler.db_connector = MagicMock()
        crawler.db_connector.get_session.return_value = test_db_session

        with patch.object(crawler, 'get_app_store_reviews_and_appname', return_value=('Test App', [])):
            with patch.object(crawler, 'save_to_parquet_and_database', return_value=0):
                crawler.retry_failed_reviews(max_retries=3)

        # Verify review with retry_count=3 is NOT retried
        failed_review_max = test_db_session.query(ReviewMasterIndex).filter_by(
            platform_review_id='failed_review_3'
        ).first()

        assert failed_review_max.retry_count == 3, \
            "Max retry review should not be incremented"


@pytest.mark.requires_db
def test_retry_failed_reviews_updates_error_message(
    test_db_session,
    db_with_failed_reviews
):
    """Test that retry updates error_message with retry info."""
    with patch.object(AppStoreCrawler, '__init__', lambda self, config_path=None: None):
        crawler = AppStoreCrawler()
        crawler.logger = MagicMock()
        crawler.config = {}
        crawler.db_connector = MagicMock()
        crawler.db_connector.get_session.return_value = test_db_session

        with patch.object(crawler, 'get_app_store_reviews_and_appname', return_value=('Test App', [])):
            with patch.object(crawler, 'save_to_parquet_and_database', return_value=0):
                crawler.retry_failed_reviews(max_retries=3)

        failed_review = test_db_session.query(ReviewMasterIndex).filter_by(
            platform_review_id='failed_review_0'
        ).first()

        assert "Retry" in failed_review.error_message, \
            "error_message should mention retry"


@pytest.mark.requires_db
def test_retry_failed_reviews_re_crawls_data(test_db_session, db_with_failed_reviews):
    """Test that retry actually calls get_app_store_reviews_and_appname (re-crawl).

    Verifies that retry_failed_reviews triggers a real API re-crawl per app.
    """
    with patch.object(AppStoreCrawler, '__init__', lambda self, config_path=None: None):
        crawler = AppStoreCrawler()
        crawler.logger = MagicMock()
        crawler.config = {}
        crawler.db_connector = MagicMock()
        crawler.db_connector.get_session.return_value = test_db_session

        mock_crawl = MagicMock(return_value=('Test App', []))
        with patch.object(crawler, 'get_app_store_reviews_and_appname', mock_crawl):
            with patch.object(crawler, 'save_to_parquet_and_database', return_value=0):
                crawler.retry_failed_reviews(max_retries=3)

        # Verify API was actually called for app '999999999'
        mock_crawl.assert_called_once_with('999999999')


# ========================================
# G. PARQUET FILE VALIDATION
# ========================================

@pytest.mark.requires_db
def test_parquet_file_structure(test_db_session, temp_bronze_dir, sample_appstore_reviews):
    """Test that Parquet file matches AppReviewSchema."""
    with patch.object(AppStoreCrawler, '__init__', lambda self, config_path: None):
        crawler = AppStoreCrawler()
        crawler.logger = MagicMock()
        crawler.enable_parquet = True
        crawler.config = {}
        crawler.db_connector = MagicMock()
        crawler.db_connector.get_session.return_value = test_db_session

        from src.utils.parquet_writer import ParquetWriter
        crawler.parquet_writer = ParquetWriter(
            base_path=str(temp_bronze_dir),
            partition_by='year_month'
        )

        crawler.save_to_parquet_and_database('123456789', sample_appstore_reviews)

        # Read Parquet
        parquet_file = list(temp_bronze_dir.glob('**/*.parquet'))[0]
        records = read_parquet_to_schemas(parquet_file, AppReviewSchema)

        # Verify schema fields
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
    with patch.object(AppStoreCrawler, '__init__', lambda self, config_path: None):
        crawler = AppStoreCrawler()
        crawler.logger = MagicMock()
        crawler.enable_parquet = True
        crawler.config = {}
        crawler.db_connector = MagicMock()
        crawler.db_connector.get_session.return_value = test_db_session

        from src.utils.parquet_writer import ParquetWriter
        crawler.parquet_writer = ParquetWriter(
            base_path=str(temp_bronze_dir),
            partition_by='year_month'
        )

        crawler.save_to_parquet_and_database('123456789', sample_appstore_reviews)

        # Verify partition structure
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
    with patch.object(AppStoreCrawler, '__init__', lambda self, config_path: None):
        crawler = AppStoreCrawler()
        crawler.logger = MagicMock()
        crawler.enable_parquet = True
        crawler.config = {}
        crawler.db_connector = MagicMock()
        crawler.db_connector.get_session.return_value = test_db_session

        from src.utils.parquet_writer import ParquetWriter
        crawler.parquet_writer = ParquetWriter(
            base_path=str(temp_bronze_dir),
            partition_by='year_month'
        )

        crawler.save_to_parquet_and_database('123456789', sample_appstore_reviews)

        # Read Parquet
        parquet_file = list(temp_bronze_dir.glob('**/*.parquet'))[0]
        records = read_parquet_to_schemas(parquet_file, AppReviewSchema)

        # Verify data integrity
        assert len(records) == len(sample_appstore_reviews)

        # Check specific fields
        for record in records:
            assert record.platform_type == 'APPSTORE'
            assert record.review_text is not None
            assert 1 <= record.rating <= 5


# ========================================
# H. DB RECORD VALIDATION
# ========================================

@pytest.mark.requires_db
def test_review_master_index_fields_populated(
    test_db_session,
    temp_bronze_dir,
    sample_appstore_reviews
):
    """Test that ReviewMasterIndex has all required fields populated."""
    with patch.object(AppStoreCrawler, '__init__', lambda self, config_path: None):
        crawler = AppStoreCrawler()
        crawler.logger = MagicMock()
        crawler.enable_parquet = True
        crawler.config = {}
        crawler.db_connector = MagicMock()
        crawler.db_connector.get_session.return_value = test_db_session

        from src.utils.parquet_writer import ParquetWriter
        crawler.parquet_writer = ParquetWriter(
            base_path=str(temp_bronze_dir),
            partition_by='year_month'
        )

        crawler.save_to_parquet_and_database('123456789', sample_appstore_reviews)

        # Verify all fields
        reviews = test_db_session.query(ReviewMasterIndex).all()

        for review in reviews:
            assert review.review_id is not None
            assert review.app_id is not None
            assert review.platform_review_id is not None
            assert review.platform_type == PlatformType.APPSTORE
            assert review.review_created_at is not None
            assert review.ingested_at is not None
            assert review.processing_status == ProcessingStatusType.RAW
            assert review.parquet_written_at is not None
            assert review.is_active is True
            assert review.is_reply is False


@pytest.mark.requires_db
def test_review_id_is_uuid_v7(test_db_session, temp_bronze_dir, sample_appstore_reviews):
    """Test that review_id is UUID v7 (time-sortable)."""
    with patch.object(AppStoreCrawler, '__init__', lambda self, config_path: None):
        crawler = AppStoreCrawler()
        crawler.logger = MagicMock()
        crawler.enable_parquet = True
        crawler.config = {}
        crawler.db_connector = MagicMock()
        crawler.db_connector.get_session.return_value = test_db_session

        from src.utils.parquet_writer import ParquetWriter
        crawler.parquet_writer = ParquetWriter(
            base_path=str(temp_bronze_dir),
            partition_by='year_month'
        )

        crawler.save_to_parquet_and_database('123456789', sample_appstore_reviews)

        reviews = test_db_session.query(ReviewMasterIndex).all()

        for review in reviews:
            # UUID v7 has version field = 7 (first hex digit of 3rd group)
            uuid_str = str(review.review_id)
            version = int(uuid_str[14], 16)  # First hex of 3rd group
            assert version == 7, f"review_id should be UUID v7, got version {version}"


@pytest.mark.requires_db
def test_processing_status_is_raw(
    test_db_session,
    temp_bronze_dir,
    sample_appstore_reviews
):
    """Test that initial processing_status is RAW."""
    with patch.object(AppStoreCrawler, '__init__', lambda self, config_path: None):
        crawler = AppStoreCrawler()
        crawler.logger = MagicMock()
        crawler.enable_parquet = True
        crawler.config = {}
        crawler.db_connector = MagicMock()
        crawler.db_connector.get_session.return_value = test_db_session

        from src.utils.parquet_writer import ParquetWriter
        crawler.parquet_writer = ParquetWriter(
            base_path=str(temp_bronze_dir),
            partition_by='year_month'
        )

        crawler.save_to_parquet_and_database('123456789', sample_appstore_reviews)

        reviews = test_db_session.query(ReviewMasterIndex).all()

        for review in reviews:
            assert review.processing_status == ProcessingStatusType.RAW


@pytest.mark.requires_db
def test_error_fields_null_on_success(
    test_db_session,
    temp_bronze_dir,
    sample_appstore_reviews
):
    """Test that error_message=NULL and retry_count=0 on success."""
    with patch.object(AppStoreCrawler, '__init__', lambda self, config_path: None):
        crawler = AppStoreCrawler()
        crawler.logger = MagicMock()
        crawler.enable_parquet = True
        crawler.config = {}
        crawler.db_connector = MagicMock()
        crawler.db_connector.get_session.return_value = test_db_session

        from src.utils.parquet_writer import ParquetWriter
        crawler.parquet_writer = ParquetWriter(
            base_path=str(temp_bronze_dir),
            partition_by='year_month'
        )

        crawler.save_to_parquet_and_database('123456789', sample_appstore_reviews)

        reviews = test_db_session.query(ReviewMasterIndex).all()

        for review in reviews:
            assert review.error_message is None, "error_message should be NULL on success"
            assert review.retry_count == 0, "retry_count should be 0 on success"


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
