"""Pydantic schema for reviews_preprocessed (Silver layer - NAS Parquet).

This schema validates preprocessed review data stored in Parquet files on NAS.
Corresponds to the reviews_preprocessed table structure in schema_v2.sql.
"""

from typing import Optional
from datetime import datetime

from pydantic import BaseModel, Field, field_validator, ConfigDict

from .base import utc_now


class ReviewPreprocessedSchema(BaseModel):
    """Schema for preprocessed review data (Silver layer).

    This schema represents review text that has been cleaned and normalized.
    Data is stored as Parquet files on NAS.

    The preprocessing pipeline:
    1. Removes profanity and spam content
    2. Normalizes synonyms
    3. Removes stopwords and special characters
    4. Stores cleaned text for downstream analysis

    Attributes:
        review_id: Foreign key to review_master_index (UUID v7)
        platform_review_id: Original platform review ID (for tracking)
        refined_text: Cleaned and normalized review text
        created_at: When preprocessing was performed
        updated_at: Last update timestamp

    Example:
        >>> preprocessed = ReviewPreprocessedSchema(
        ...     review_id="123e4567-e89b-12d3-a456-426614174000",
        ...     platform_review_id="12345678",
        ...     refined_text="Great banking app with excellent features"
        ... )
    """

    review_id: str = Field(
        ...,
        description="Foreign key to review_master_index (UUID v7)"
    )
    platform_review_id: str = Field(
        ...,
        description="Original platform review ID"
    )
    refined_text: Optional[str] = Field(
        None,
        description="Cleaned and normalized review text"
    )
    created_at: datetime = Field(
        default_factory=utc_now,
        description="When preprocessing was performed"
    )
    updated_at: datetime = Field(
        default_factory=utc_now,
        description="Last update timestamp"
    )

    @field_validator('review_id', 'platform_review_id')
    @classmethod
    def validate_id_not_empty(cls, v: str) -> str:
        """Validate ID fields are not empty.

        Args:
            v: ID string

        Returns:
            str: Validated ID

        Raises:
            ValueError: If ID is empty
        """
        if not v or not v.strip():
            raise ValueError("ID fields cannot be empty")
        return v

    @field_validator('refined_text')
    @classmethod
    def validate_refined_text(cls, v: Optional[str]) -> Optional[str]:
        """Validate refined_text.

        Refined text can be None (if preprocessing failed) but if present,
        it should not be an empty string.

        Args:
            v: Refined text or None

        Returns:
            Optional[str]: Validated refined text or None

        Raises:
            ValueError: If refined text is empty string (but not None)
        """
        if v is not None and not v.strip():
            raise ValueError("refined_text cannot be empty string (use None instead)")
        return v

    model_config = ConfigDict(
        # Allow arbitrary types for datetime
        arbitrary_types_allowed=True,
    )


# Type alias for convenience
ReviewPreprocessed = ReviewPreprocessedSchema
