"""Parquet schema definitions for NAS storage.

This package contains Pydantic schemas for validating data stored in Parquet
files on NAS. These schemas correspond to the Bronze and Silver layer tables
in the Medallion architecture.

Bronze Layer (Raw Data):
    - AppReviewSchema: Raw reviews from App Store and Play Store

Silver Layer (Processed Data):
    - ReviewPreprocessedSchema: Cleaned and normalized review text

Usage:
    >>> from src.schemas.parquet import AppReviewSchema, ReviewPreprocessedSchema
    >>> from datetime import datetime, timezone
    >>>
    >>> # Create a raw review
    >>> review = AppReviewSchema(
    ...     app_id="123e4567-e89b-12d3-a456-426614174000",
    ...     platform_type="APPSTORE",
    ...     platform_review_id="12345678",
    ...     review_text="Great app!",
    ...     rating=5,
    ...     reviewed_at=datetime.now(timezone.utc)
    ... )
    >>>
    >>> # Create preprocessed review
    >>> preprocessed = ReviewPreprocessedSchema(
    ...     review_id=review.review_id,
    ...     platform_review_id=review.platform_review_id,
    ...     refined_text="great app"
    ... )
"""

from .base import generate_uuid_v7, utc_now, to_utc
from .app_review import AppReviewSchema, AppReview
from .review_preprocessed import ReviewPreprocessedSchema, ReviewPreprocessed

__all__ = [
    # Utilities
    'generate_uuid_v7',
    'utc_now',
    'to_utc',
    # Bronze Layer
    'AppReviewSchema',
    'AppReview',
    # Silver Layer
    'ReviewPreprocessedSchema',
    'ReviewPreprocessed',
]
