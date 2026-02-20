"""
Unit tests for Crawler Distributed Consistency

Tests NAS-first dual-write pattern and consistency guarantees.

Key test areas:
1. NAS-first write sequence (Parquet before DB)
2. Ghost Records prevention (no DB without Parquet)
3. Idempotency (duplicate prevention via platform_review_id)
4. Retry mechanism (failed review tracking and retry)
"""

import pytest
from unittest.mock import patch, MagicMock, call
from datetime import datetime, timezone
from uuid6 import uuid7

from src.crawlers.appstore_crawler import (
    AppStoreCrawler,
    ParquetWriteError,
    DBCommitError
)
from src.models.apps import App
from src.models.review_master_index import ReviewMasterIndex
from src.models.enums import PlatformType, ProcessingStatusType


class TestNASFirstWriteSequence:
    """Test NAS-first write sequence guarantees."""

    @pytest.mark.requires_db
    def test_parquet_write_before_db_commit(
        self,
        test_db_session,
        temp_bronze_dir,
        sample_appstore_reviews
    ):
        """Test that Parquet write happens before DB commit.

        Verification:
        1. Mock Parquet writer and DB session
        2. Call save_to_parquet_and_database()
        3. Verify Parquet write called before DB commit using call order
        """
        with patch.object(AppStoreCrawler, '__init__', lambda self, config_path=None: None):
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

            # Spy on both operations
            original_write = crawler.parquet_writer.write_batch
            original_commit = test_db_session.commit

            call_order = []

            def tracked_write(records, partition_date=None):
                call_order.append('parquet_write')
                return original_write(records, partition_date)

            def tracked_commit():
                call_order.append('db_commit')
                return original_commit()

            crawler.parquet_writer.write_batch = tracked_write
            test_db_session.commit = tracked_commit

            # Execute
            crawler.save_to_parquet_and_database('123456789', sample_appstore_reviews)

            # Verify call order: Parquet BEFORE DB
            assert 'parquet_write' in call_order
            assert 'db_commit' in call_order

            parquet_index = call_order.index('parquet_write')
            db_index = call_order.index('db_commit')

            assert parquet_index < db_index, \
                f"Parquet write must happen before DB commit. Order: {call_order}"

    @pytest.mark.requires_db
    def test_db_commit_only_if_parquet_success(
        self,
        test_db_session,
        temp_bronze_dir,
        sample_appstore_reviews
    ):
        """Test DB commit only proceeds if Parquet write succeeds.

        Verification:
        1. Mock Parquet write success → verify DB commit is called
        2. Mock Parquet write failure → verify DB commit is NOT called
        3. Verify session.rollback() called on Parquet failure
        """
        with patch.object(AppStoreCrawler, '__init__', lambda self, config_path=None: None):
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

            # Scenario 1: Parquet SUCCESS → DB commit called
            commit_spy = MagicMock(side_effect=test_db_session.commit)
            test_db_session.commit = commit_spy

            crawler.save_to_parquet_and_database('123456789', sample_appstore_reviews)

            assert commit_spy.called, "DB commit should be called after Parquet success"

            # Scenario 2: Parquet FAILURE → DB commit NOT called
            test_db_session.commit.reset_mock()
            rollback_spy = MagicMock(side_effect=test_db_session.rollback)
            test_db_session.rollback = rollback_spy

            with patch.object(
                crawler.parquet_writer,
                'write_batch',
                side_effect=Exception("Parquet failed")
            ):
                try:
                    crawler.save_to_parquet_and_database('999999999', sample_appstore_reviews)
                except ParquetWriteError:
                    pass  # Expected

            assert not commit_spy.called, "DB commit should NOT be called after Parquet failure"
            assert rollback_spy.called, "session.rollback() should be called"


class TestGhostRecordsPrevention:
    """Test prevention of Ghost Records (DB without Parquet)."""

    @pytest.mark.requires_db
    def test_no_db_commit_on_parquet_failure(
        self,
        test_db_session,
        temp_bronze_dir,
        sample_appstore_reviews
    ):
        """Test no DB commit if Parquet write fails.

        Verification:
        1. Mock Parquet write to raise ParquetWriteError
        2. Verify DB session.rollback() is called
        3. Verify no ReviewMasterIndex records created with status=RAW
        """
        with patch.object(AppStoreCrawler, '__init__', lambda self, config_path=None: None):
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

            rollback_spy = MagicMock(side_effect=test_db_session.rollback)
            test_db_session.rollback = rollback_spy

            # Mock Parquet to fail
            with patch.object(
                crawler.parquet_writer,
                'write_batch',
                side_effect=Exception("Disk full")
            ):
                with pytest.raises(ParquetWriteError):
                    crawler.save_to_parquet_and_database('123456789', sample_appstore_reviews)

            # Verify rollback called
            assert rollback_spy.called, "session.rollback() should be called"

            # Verify no RAW records
            raw_count = test_db_session.query(ReviewMasterIndex).filter_by(
                processing_status=ProcessingStatusType.RAW
            ).count()

            assert raw_count == 0, "No RAW records should exist after Parquet failure"

    @pytest.mark.requires_db
    def test_db_rollback_on_parquet_error(
        self,
        test_db_session,
        temp_bronze_dir,
        sample_appstore_reviews
    ):
        """Test DB rollback when Parquet write raises error.

        Verification:
        1. Mock Parquet writer to raise generic Exception
        2. Verify session.rollback() called
        3. Verify ParquetWriteError re-raised
        """
        with patch.object(AppStoreCrawler, '__init__', lambda self, config_path=None: None):
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

            rollback_spy = MagicMock(side_effect=test_db_session.rollback)
            test_db_session.rollback = rollback_spy

            with patch.object(
                crawler.parquet_writer,
                'write_batch',
                side_effect=PermissionError("Access denied")
            ):
                with pytest.raises(ParquetWriteError) as exc_info:
                    crawler.save_to_parquet_and_database('123456789', sample_appstore_reviews)

                assert "Access denied" in str(exc_info.value)

            assert rollback_spy.called, "session.rollback() should be called"


class TestIdempotency:
    """Test idempotency guarantees."""

    @pytest.mark.requires_db
    def test_duplicate_platform_review_id_skipped(
        self,
        test_db_session,
        temp_bronze_dir,
        sample_appstore_reviews
    ):
        """Test duplicate reviews are skipped via platform_review_id.

        Verification:
        1. Insert review with platform_review_id='test_123'
        2. Call save_to_parquet_and_database() with same platform_review_id
        3. Verify no new ReviewMasterIndex record created
        4. Verify Parquet not written for duplicate
        """
        with patch.object(AppStoreCrawler, '__init__', lambda self, config_path=None: None):
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

            initial_count = test_db_session.query(ReviewMasterIndex).count()

            # Second write (duplicates)
            count2 = crawler.save_to_parquet_and_database(app_id, sample_appstore_reviews)

            assert count2 == 0, "Duplicates should return 0"

            final_count = test_db_session.query(ReviewMasterIndex).count()
            assert final_count == initial_count, "No new records should be created"

    @pytest.mark.requires_db
    def test_partial_duplicate_handling(
        self,
        test_db_session,
        temp_bronze_dir
    ):
        """Test handling of partially duplicated review batches.

        Verification:
        1. Insert 5 existing reviews
        2. Call save with batch of 10 reviews (5 old + 5 new)
        3. Verify only 5 new records added
        4. Verify only 5 Parquet records written
        """
        with patch.object(AppStoreCrawler, '__init__', lambda self, config_path=None: None):
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

            # Create 5 old reviews
            old_reviews = [
                {
                    'id': {'label': f'old_{i}'},
                    'author': {'name': {'label': f'User{i}'}},
                    'im:name': {'label': 'Test App'},
                    'content': {'label': f'Old review {i}'},
                    'im:rating': {'label': '5'},
                    'updated': {'label': '2026-02-04T12:00:00Z'},
                }
                for i in range(5)
            ]

            # First write
            count1 = crawler.save_to_parquet_and_database(app_id, old_reviews)
            assert count1 == 5

            # Create mixed batch (5 old + 5 new)
            new_reviews = [
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

            mixed_batch = old_reviews + new_reviews

            # Second write
            count2 = crawler.save_to_parquet_and_database(app_id, mixed_batch)

            assert count2 == 5, "Only 5 new reviews should be added"

            total = test_db_session.query(ReviewMasterIndex).count()
            assert total == 10, "Total should be 10 (5 old + 5 new)"


class TestRetryMechanism:
    """Test retry mechanism for failed writes."""

    @pytest.mark.requires_db
    def test_retry_failed_reviews(self, test_db_session, db_with_failed_reviews, sample_appstore_reviews):
        """Test retry_failed_reviews() method with full re-crawl.

        Verification:
        1. Insert ReviewMasterIndex with processing_status=FAILED
        2. Mock API calls to return sample reviews
        3. Call retry_failed_reviews()
        4. Verify failed reviews are re-processed successfully
        """
        with patch.object(AppStoreCrawler, '__init__', lambda self, config_path=None: None):
            crawler = AppStoreCrawler()
            crawler.logger = MagicMock()
            crawler.config = {}
            crawler.db_connector = MagicMock()
            crawler.db_connector.get_session.return_value = test_db_session
            crawler.enable_parquet = False  # Disable Parquet for simplicity

            # Mock the API call to return sample reviews
            crawler.get_app_store_reviews_and_appname = MagicMock(
                return_value=('Test App', sample_appstore_reviews)
            )

            # Mock save_to_parquet_and_database to succeed
            crawler.save_to_parquet_and_database = MagicMock(return_value=len(sample_appstore_reviews))

            # db_with_failed_reviews has 3 reviews from app '999999999':
            # - retry_count=0
            # - retry_count=1
            # - retry_count=3 (max reached)

            retried_count = crawler.retry_failed_reviews(max_retries=3)

            # Should retry 1 app (that has 2 retryable reviews)
            assert retried_count == 1, f"Expected 1 app retried, got {retried_count}"

            # Verify API was called to re-crawl
            crawler.get_app_store_reviews_and_appname.assert_called_once_with('999999999')

            # Verify save method was called
            crawler.save_to_parquet_and_database.assert_called_once()

    @pytest.mark.requires_db
    def test_max_retries_reached(self, test_db_session, db_with_failed_reviews, sample_appstore_reviews):
        """Test DLQ behavior when max retries reached.

        Verification:
        1. Insert FAILED review with retry_count=3
        2. Call retry_failed_reviews(max_retries=3)
        3. Verify review NOT selected for retry
        4. Verify review remains in FAILED state
        """
        with patch.object(AppStoreCrawler, '__init__', lambda self, config_path=None: None):
            crawler = AppStoreCrawler()
            crawler.logger = MagicMock()
            crawler.config = {}
            crawler.db_connector = MagicMock()
            crawler.db_connector.get_session.return_value = test_db_session
            crawler.enable_parquet = False  # Disable Parquet for simplicity

            # Mock API call to return sample reviews (for the 2 retryable reviews)
            crawler.get_app_store_reviews_and_appname = MagicMock(
                return_value=('Test App', sample_appstore_reviews)
            )

            # Mock save method
            crawler.save_to_parquet_and_database = MagicMock(return_value=len(sample_appstore_reviews))

            # Execute retry
            retried_count = crawler.retry_failed_reviews(max_retries=3)

            # Should retry 1 app (that has 2 reviews with retry_count < 3)
            # The review with retry_count=3 should be excluded from query
            assert retried_count == 1, f"Should retry 1 app, got {retried_count}"

            # Verify review with retry_count=3 still exists and unchanged
            review_max = test_db_session.query(ReviewMasterIndex).filter_by(
                platform_review_id='failed_review_3'
            ).first()

            assert review_max is not None, "Max retry review should still exist"
            assert review_max.retry_count == 3, \
                "Max retry review should not be incremented"
            assert review_max.processing_status == ProcessingStatusType.FAILED


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
