"""Test Pydantic schemas for Parquet files.

This module tests:
1. Schema validation for AppReviewSchema
2. Schema validation for ReviewPreprocessedSchema
3. Invalid data rejection
4. Field validators
"""

import pytest
from datetime import datetime, timezone
from pydantic import ValidationError


def test_app_review_schema_valid():
    """Test AppReviewSchema with valid data."""
    from src.schemas.parquet import AppReviewSchema

    review = AppReviewSchema(
        app_id="123e4567-e89b-12d3-a456-426614174000",
        platform_type="APPSTORE",
        platform_review_id="12345678",
        review_text="Great banking app!",
        rating=5,
        reviewed_at=datetime.now(timezone.utc)
    )

    assert review.app_id == "123e4567-e89b-12d3-a456-426614174000"
    assert review.platform_type == "APPSTORE"
    assert review.rating == 5
    assert review.review_id is not None  # Auto-generated
    assert review.created_at is not None  # Auto-generated


def test_app_review_schema_invalid_platform():
    """Test AppReviewSchema rejects invalid platform type."""
    from src.schemas.parquet import AppReviewSchema

    with pytest.raises(ValidationError) as exc_info:
        AppReviewSchema(
            app_id="123e4567-e89b-12d3-a456-426614174000",
            platform_type="INVALID_PLATFORM",  # Invalid
            platform_review_id="12345678",
            review_text="Great app!",
            rating=5,
            reviewed_at=datetime.now(timezone.utc)
        )

    assert "platform_type" in str(exc_info.value)


def test_app_review_schema_invalid_rating():
    """Test AppReviewSchema rejects invalid rating."""
    from src.schemas.parquet import AppReviewSchema

    # Rating too low
    with pytest.raises(ValidationError):
        AppReviewSchema(
            app_id="123e4567-e89b-12d3-a456-426614174000",
            platform_type="APPSTORE",
            platform_review_id="12345678",
            review_text="Bad app",
            rating=0,  # Invalid
            reviewed_at=datetime.now(timezone.utc)
        )

    # Rating too high
    with pytest.raises(ValidationError):
        AppReviewSchema(
            app_id="123e4567-e89b-12d3-a456-426614174000",
            platform_type="APPSTORE",
            platform_review_id="12345678",
            review_text="Amazing app",
            rating=6,  # Invalid
            reviewed_at=datetime.now(timezone.utc)
        )


def test_app_review_schema_empty_review_text():
    """Test AppReviewSchema rejects empty review text."""
    from src.schemas.parquet import AppReviewSchema

    with pytest.raises(ValidationError) as exc_info:
        AppReviewSchema(
            app_id="123e4567-e89b-12d3-a456-426614174000",
            platform_type="APPSTORE",
            platform_review_id="12345678",
            review_text="",  # Empty
            rating=5,
            reviewed_at=datetime.now(timezone.utc)
        )

    assert "review_text" in str(exc_info.value)


def test_app_review_schema_playstore():
    """Test AppReviewSchema with Play Store data."""
    from src.schemas.parquet import AppReviewSchema

    review = AppReviewSchema(
        app_id="123e4567-e89b-12d3-a456-426614174000",
        platform_type="PLAYSTORE",
        platform_review_id="gp:12345",
        reviewer_name="John Doe",
        review_text="Excellent app for banking",
        rating=4,
        reviewed_at=datetime.now(timezone.utc),
        is_reply=False
    )

    assert review.platform_type == "PLAYSTORE"
    assert review.reviewer_name == "John Doe"
    assert review.is_reply is False


def test_app_review_schema_with_reply():
    """Test AppReviewSchema with developer reply."""
    from src.schemas.parquet import AppReviewSchema

    review = AppReviewSchema(
        app_id="123e4567-e89b-12d3-a456-426614174000",
        platform_type="APPSTORE",
        platform_review_id="12345678",
        review_text="App keeps crashing",
        rating=1,
        reviewed_at=datetime.now(timezone.utc),
        is_reply=True,
        reply_comment="Thank you for your feedback. We've fixed the issue in v2.1"
    )

    assert review.is_reply is True
    assert review.reply_comment is not None
    assert "fixed" in review.reply_comment


def test_review_preprocessed_schema_valid():
    """Test ReviewPreprocessedSchema with valid data."""
    from src.schemas.parquet import ReviewPreprocessedSchema

    preprocessed = ReviewPreprocessedSchema(
        review_id="123e4567-e89b-12d3-a456-426614174000",
        platform_review_id="12345678",
        refined_text="great banking app excellent feature"
    )

    assert preprocessed.review_id == "123e4567-e89b-12d3-a456-426614174000"
    assert preprocessed.refined_text == "great banking app excellent feature"
    assert preprocessed.created_at is not None
    assert preprocessed.updated_at is not None


def test_review_preprocessed_schema_empty_ids():
    """Test ReviewPreprocessedSchema rejects empty IDs."""
    from src.schemas.parquet import ReviewPreprocessedSchema

    # Empty review_id
    with pytest.raises(ValidationError) as exc_info:
        ReviewPreprocessedSchema(
            review_id="",  # Empty
            platform_review_id="12345678",
            refined_text="cleaned text"
        )

    assert "review_id" in str(exc_info.value)

    # Empty platform_review_id
    with pytest.raises(ValidationError) as exc_info:
        ReviewPreprocessedSchema(
            review_id="123e4567-e89b-12d3-a456-426614174000",
            platform_review_id="",  # Empty
            refined_text="cleaned text"
        )

    assert "platform_review_id" in str(exc_info.value)


def test_review_preprocessed_schema_null_refined_text():
    """Test ReviewPreprocessedSchema accepts None for refined_text."""
    from src.schemas.parquet import ReviewPreprocessedSchema

    # None is allowed (preprocessing might fail)
    preprocessed = ReviewPreprocessedSchema(
        review_id="123e4567-e89b-12d3-a456-426614174000",
        platform_review_id="12345678",
        refined_text=None
    )

    assert preprocessed.refined_text is None


def test_review_preprocessed_schema_empty_refined_text():
    """Test ReviewPreprocessedSchema rejects empty string for refined_text."""
    from src.schemas.parquet import ReviewPreprocessedSchema

    # Empty string is not allowed (use None instead)
    with pytest.raises(ValidationError) as exc_info:
        ReviewPreprocessedSchema(
            review_id="123e4567-e89b-12d3-a456-426614174000",
            platform_review_id="12345678",
            refined_text=""  # Empty string not allowed
        )

    assert "refined_text" in str(exc_info.value)


def test_app_review_schema_model_dump():
    """Test AppReviewSchema serialization to dict."""
    from src.schemas.parquet import AppReviewSchema

    review = AppReviewSchema(
        app_id="123e4567-e89b-12d3-a456-426614174000",
        platform_type="APPSTORE",
        platform_review_id="12345678",
        review_text="Great app!",
        rating=5,
        reviewed_at=datetime(2026, 2, 4, 12, 0, 0, tzinfo=timezone.utc)
    )

    data = review.model_dump()

    assert isinstance(data, dict)
    assert data['app_id'] == "123e4567-e89b-12d3-a456-426614174000"
    assert data['platform_type'] == "APPSTORE"
    assert data['rating'] == 5
    assert 'review_id' in data
    assert 'created_at' in data


def test_uuid_generation_uniqueness():
    """Test that review_id is unique across multiple instances."""
    from src.schemas.parquet import AppReviewSchema

    reviews = [
        AppReviewSchema(
            app_id="123e4567-e89b-12d3-a456-426614174000",
            platform_type="APPSTORE",
            platform_review_id=f"review_{i}",
            review_text=f"Review {i}",
            rating=5,
            reviewed_at=datetime.now(timezone.utc)
        )
        for i in range(10)
    ]

    review_ids = [r.review_id for r in reviews]

    # All IDs should be unique
    assert len(review_ids) == len(set(review_ids))


def test_timestamp_generation():
    """Test that timestamps are auto-generated in UTC."""
    from src.schemas.parquet import AppReviewSchema
    from datetime import datetime, timezone

    before = datetime.now(timezone.utc)

    review = AppReviewSchema(
        app_id="123e4567-e89b-12d3-a456-426614174000",
        platform_type="APPSTORE",
        platform_review_id="12345678",
        review_text="Test review",
        rating=5,
        reviewed_at=datetime.now(timezone.utc)
    )

    after = datetime.now(timezone.utc)

    # created_at should be between before and after
    assert before <= review.created_at <= after
    assert review.created_at.tzinfo is not None


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
