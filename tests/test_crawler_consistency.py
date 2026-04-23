"""Crawler consistency tests for the Batch DLQ architecture.

The crawler writes one daily Bronze parquet batch to MinIO and registers that
batch in ingestion_batch as PENDING. ReviewMasterIndex rows are created later by
the load stage, so these tests focus on batch registration and cleanup behavior.
"""

from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from src.crawlers.appstore_crawler import AppStoreCrawler, ParquetWriteError
from src.models.enums import IngestionBatchStatusType, PlatformType
from src.models.ingestion_batch import IngestionBatch


def _make_crawler(session):
    """Create an AppStoreCrawler test double with only save_daily_batch dependencies."""
    with patch.object(AppStoreCrawler, "__init__", lambda self, config_path=None: None):
        crawler = AppStoreCrawler()
    crawler.logger = MagicMock()
    crawler.enable_parquet = True
    crawler.max_retries = 3
    crawler._minio = MagicMock()
    crawler.db_connector = MagicMock()
    crawler.db_connector.get_session.return_value = session
    return crawler


def _records(count=2):
    """Return schema-like records accepted by BaseCrawler.save_daily_batch."""
    return [
        SimpleNamespace(
            model_dump=lambda i=i: {
                "review_id": f"019db8c0-a18{i}-7000-8000-00000000000{i}",
                "app_id": "019db8c0-a180-7000-8000-000000000001",
                "platform_type": "APPSTORE",
                "platform_review_id": f"review_{i}",
                "reviewer_name": f"User {i}",
                "review_text": f"Review text {i}",
                "rating": 5,
                "reviewed_at": datetime(2026, 3, 4, 12, i, tzinfo=timezone.utc),
                "is_reply": False,
                "reply_comment": None,
            }
        )
        for i in range(count)
    ]


@pytest.mark.requires_db
def test_save_daily_batch_uploads_parquet_before_registering_pending_batch(test_db_session):
    """Successful crawl output creates a MinIO parquet file before PENDING batch commit."""
    crawler = _make_crawler(test_db_session)
    partition_date = datetime(2026, 3, 4, tzinfo=timezone.utc)

    batch_id, count, s3_key = crawler.save_daily_batch(
        _records(2),
        PlatformType.APPSTORE,
        partition_date=partition_date,
    )

    assert count == 2
    assert s3_key.startswith("bronze/app_reviews/year=2026/month=03/day=04/appstore_")
    crawler._minio.put_parquet.assert_called_once()
    assert crawler._minio.put_parquet.call_args.args[0] == s3_key

    batch = test_db_session.query(IngestionBatch).filter_by(batch_id=batch_id).one()
    assert batch.source_type == PlatformType.APPSTORE
    assert batch.platform_app_id == "daily_batch"
    assert batch.storage_path == s3_key
    assert batch.file_format == "parquet"
    assert batch.record_count == 2
    assert batch.status == IngestionBatchStatusType.PENDING
    assert batch.retry_count == 0
    assert batch.max_retries == 3


def test_save_daily_batch_does_not_open_db_session_when_minio_upload_fails():
    """MinIO failure prevents ghost ingestion_batch records."""
    crawler = _make_crawler(MagicMock())
    crawler._minio.put_parquet.side_effect = RuntimeError("upload down")

    with pytest.raises(ParquetWriteError, match="upload down"):
        crawler.save_daily_batch(_records(1), PlatformType.APPSTORE)

    crawler.db_connector.get_session.assert_not_called()


def test_save_daily_batch_cleans_up_minio_object_when_batch_commit_fails():
    """DB registration failure rolls back and deletes the already uploaded parquet object."""
    session = MagicMock()
    session.commit.side_effect = RuntimeError("commit down")
    crawler = _make_crawler(session)

    with pytest.raises(RuntimeError, match="commit down"):
        crawler.save_daily_batch(_records(1), PlatformType.APPSTORE)

    s3_key = crawler._minio.put_parquet.call_args.args[0]
    session.rollback.assert_called_once()
    crawler._minio.delete_object.assert_called_once_with(s3_key)
    session.close.assert_called_once()


def test_save_daily_batch_returns_empty_result_without_side_effects_for_empty_records():
    """Empty crawl output skips MinIO and DB work."""
    crawler = _make_crawler(MagicMock())

    assert crawler.save_daily_batch([], PlatformType.APPSTORE) == (None, 0, None)

    crawler._minio.put_parquet.assert_not_called()
    crawler.db_connector.get_session.assert_not_called()
