"""Test Parquet writer utility.

This module tests:
1. Parquet file writing with different partitioning strategies
2. Batch writing
3. File reading and validation
4. Partition management
"""

import pytest
import tempfile
import shutil
from pathlib import Path
from datetime import datetime, timezone

from src.schemas.parquet import AppReviewSchema, ReviewPreprocessedSchema
from src.utils.parquet_writer import ParquetWriter, read_parquet_to_schemas


@pytest.fixture
def temp_dir():
    """Create a temporary directory for testing."""
    temp_path = tempfile.mkdtemp()
    yield Path(temp_path)
    # Cleanup
    shutil.rmtree(temp_path)


@pytest.fixture
def sample_reviews():
    """Create sample review data for testing."""
    return [
        AppReviewSchema(
            app_id="123e4567-e89b-12d3-a456-426614174000",
            platform_type="APPSTORE",
            platform_review_id=f"review_{i}",
            review_text=f"Test review {i}",
            rating=5,
            reviewed_at=datetime(2026, 2, 4, 12, i, 0, tzinfo=timezone.utc)
        )
        for i in range(5)
    ]


def test_parquet_writer_no_partition(temp_dir, sample_reviews):
    """Test writing Parquet files without partitioning."""
    writer = ParquetWriter(
        base_path=temp_dir / "reviews",
        partition_by='none'
    )

    file_path = writer.write_batch(sample_reviews)

    # Verify file was created
    assert file_path.exists()
    assert file_path.suffix == '.parquet'

    # Verify can read back
    read_reviews = read_parquet_to_schemas(file_path, AppReviewSchema)
    assert len(read_reviews) == len(sample_reviews)


def test_parquet_writer_year_partition(temp_dir, sample_reviews):
    """Test writing Parquet files with year partitioning."""
    writer = ParquetWriter(
        base_path=temp_dir / "reviews",
        partition_by='year'
    )

    partition_date = datetime(2026, 2, 4, tzinfo=timezone.utc)
    file_path = writer.write_batch(sample_reviews, partition_date)

    # Verify partition directory structure
    assert "year=2026" in str(file_path)
    assert file_path.exists()

    # Verify can read back
    read_reviews = read_parquet_to_schemas(file_path, AppReviewSchema)
    assert len(read_reviews) == len(sample_reviews)


def test_parquet_writer_year_month_partition(temp_dir, sample_reviews):
    """Test writing Parquet files with year-month partitioning."""
    writer = ParquetWriter(
        base_path=temp_dir / "reviews",
        partition_by='year_month'
    )

    partition_date = datetime(2026, 2, 4, tzinfo=timezone.utc)
    file_path = writer.write_batch(sample_reviews, partition_date)

    # Verify partition directory structure
    assert "year=2026" in str(file_path)
    assert "month=02" in str(file_path)
    assert file_path.exists()


def test_parquet_writer_year_month_day_partition(temp_dir, sample_reviews):
    """Test writing Parquet files with year-month-day partitioning."""
    writer = ParquetWriter(
        base_path=temp_dir / "reviews",
        partition_by='year_month_day'
    )

    partition_date = datetime(2026, 2, 4, tzinfo=timezone.utc)
    file_path = writer.write_batch(sample_reviews, partition_date)

    # Verify partition directory structure
    assert "year=2026" in str(file_path)
    assert "month=02" in str(file_path)
    assert "day=04" in str(file_path)
    assert file_path.exists()


def test_parquet_writer_empty_batch(temp_dir):
    """Test that writing empty batch raises error."""
    writer = ParquetWriter(
        base_path=temp_dir / "reviews",
        partition_by='none'
    )

    with pytest.raises(ValueError, match="Cannot write empty batch"):
        writer.write_batch([])


def test_parquet_writer_single_record(temp_dir):
    """Test writing a single record."""
    writer = ParquetWriter(
        base_path=temp_dir / "reviews",
        partition_by='none'
    )

    review = AppReviewSchema(
        app_id="123e4567-e89b-12d3-a456-426614174000",
        platform_type="APPSTORE",
        platform_review_id="single_review",
        review_text="Single test review",
        rating=5,
        reviewed_at=datetime.now(timezone.utc)
    )

    file_path = writer.write_single(review)

    assert file_path.exists()

    # Verify can read back
    read_reviews = read_parquet_to_schemas(file_path, AppReviewSchema)
    assert len(read_reviews) == 1
    assert read_reviews[0].platform_review_id == "single_review"


def test_parquet_writer_multiple_batches(temp_dir, sample_reviews):
    """Test writing multiple batches to same partition."""
    writer = ParquetWriter(
        base_path=temp_dir / "reviews",
        partition_by='year_month'
    )

    partition_date = datetime(2026, 2, 4, tzinfo=timezone.utc)

    # Write first batch
    file_path_1 = writer.write_batch(sample_reviews[:3], partition_date)

    # Write second batch to same partition
    file_path_2 = writer.append_to_partition(sample_reviews[3:], partition_date)

    # Both files should exist
    assert file_path_1.exists()
    assert file_path_2.exists()

    # Should be different files in same partition
    assert file_path_1.parent == file_path_2.parent
    assert file_path_1 != file_path_2


def test_parquet_writer_list_partitions(temp_dir, sample_reviews):
    """Test listing partition directories."""
    writer = ParquetWriter(
        base_path=temp_dir / "reviews",
        partition_by='year_month'
    )

    # Write to multiple partitions
    writer.write_batch(sample_reviews, datetime(2026, 1, 15, tzinfo=timezone.utc))
    writer.write_batch(sample_reviews, datetime(2026, 2, 4, tzinfo=timezone.utc))

    partitions = writer.list_partitions()

    assert len(partitions) == 2
    assert any("year=2026" in str(p) and "month=01" in str(p) for p in partitions)
    assert any("year=2026" in str(p) and "month=02" in str(p) for p in partitions)


def test_parquet_writer_partition_stats(temp_dir, sample_reviews):
    """Test getting partition statistics."""
    writer = ParquetWriter(
        base_path=temp_dir / "reviews",
        partition_by='year_month'
    )

    # Write some data
    writer.write_batch(sample_reviews, datetime(2026, 2, 4, tzinfo=timezone.utc))

    stats = writer.get_partition_stats()

    assert stats['num_partitions'] == 1
    assert stats['num_files'] == 1
    assert stats['total_size_bytes'] > 0
    assert stats['total_size_mb'] > 0
    assert len(stats['partitions']) == 1


def test_parquet_writer_compression(temp_dir, sample_reviews):
    """Test different compression codecs."""
    for compression in ['snappy', 'gzip', 'none']:
        writer = ParquetWriter(
            base_path=temp_dir / f"reviews_{compression}",
            partition_by='none',
            compression=compression
        )

        file_path = writer.write_batch(sample_reviews)
        assert file_path.exists()

        # Verify can read back
        read_reviews = read_parquet_to_schemas(file_path, AppReviewSchema)
        assert len(read_reviews) == len(sample_reviews)


def test_parquet_writer_preprocessed_schema(temp_dir):
    """Test writing ReviewPreprocessedSchema."""
    writer = ParquetWriter(
        base_path=temp_dir / "preprocessed",
        partition_by='year_month'
    )

    preprocessed_reviews = [
        ReviewPreprocessedSchema(
            review_id=f"123e4567-e89b-12d3-a456-42661417400{i}",
            platform_review_id=f"review_{i}",
            refined_text=f"cleaned review text {i}"
        )
        for i in range(3)
    ]

    file_path = writer.write_batch(
        preprocessed_reviews,
        datetime(2026, 2, 4, tzinfo=timezone.utc)
    )

    assert file_path.exists()

    # Verify can read back
    read_reviews = read_parquet_to_schemas(file_path, ReviewPreprocessedSchema)
    assert len(read_reviews) == len(preprocessed_reviews)
    assert read_reviews[0].refined_text.startswith("cleaned review text")


def test_parquet_writer_data_persistence(temp_dir, sample_reviews):
    """Test that data is correctly persisted and matches original."""
    writer = ParquetWriter(
        base_path=temp_dir / "reviews",
        partition_by='none'
    )

    file_path = writer.write_batch(sample_reviews)

    # Read back and verify all fields match
    read_reviews = read_parquet_to_schemas(file_path, AppReviewSchema)

    for original, read_back in zip(sample_reviews, read_reviews):
        assert original.app_id == read_back.app_id
        assert original.platform_type == read_back.platform_type
        assert original.platform_review_id == read_back.platform_review_id
        assert original.review_text == read_back.review_text
        assert original.rating == read_back.rating


def test_parquet_writer_base_path_creation(temp_dir):
    """Test that base path is created if it doesn't exist."""
    non_existent_path = temp_dir / "deep" / "nested" / "path"

    assert not non_existent_path.exists()

    writer = ParquetWriter(
        base_path=non_existent_path,
        partition_by='none'
    )

    # Base path should now exist
    assert non_existent_path.exists()


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
