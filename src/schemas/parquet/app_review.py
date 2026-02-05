"""Pydantic schema for app_reviews (Bronze layer - NAS Parquet).

This schema validates raw review data stored in Parquet files on NAS.
Corresponds to the app_reviews table structure in schema_v2.sql.
"""

from typing import Optional
from datetime import datetime

from pydantic import BaseModel, Field, field_validator, ConfigDict

from .base import generate_uuid_v7, utc_now


class AppReviewSchema(BaseModel):
    """Schema for raw app review data (Bronze layer).

    This schema represents the raw review data crawled from App Store and Play Store.
    Data is stored as Parquet files on NAS for efficient storage and retrieval.

    Attributes:
        review_id: Unique review identifier (UUID v7)
        app_id: Foreign key to apps table (UUID)
        platform_type: Platform source (APPSTORE or PLAYSTORE)
        platform_review_id: Original review ID from the platform
        reviewer_name: Name of the reviewer (optional)
        review_text: Review content text
        rating: Star rating (1-5)
        reviewed_at: When the review was written on the platform
        created_at: When the review was ingested into our system
        is_reply: Whether this is a developer reply
        reply_comment: Developer's reply text (if applicable)

    Example:
        >>> review = AppReviewSchema(
        ...     app_id="123e4567-e89b-12d3-a456-426614174000",
        ...     platform_type="APPSTORE",
        ...     platform_review_id="12345678",
        ...     review_text="Great app!",
        ...     rating=5,
        ...     reviewed_at=datetime.now(timezone.utc)
        ... )
    """

    review_id: str = Field(
        default_factory=generate_uuid_v7,
        description="Unique review identifier (UUID v7)"
    )
    app_id: str = Field(
        ...,
        description="Foreign key to apps table (UUID)"
    )
    platform_type: str = Field(
        ...,
        description="Platform source (APPSTORE or PLAYSTORE)"
    )
    platform_review_id: str = Field(
        ...,
        description="Original review ID from the platform"
    )
    reviewer_name: Optional[str] = Field(
        None,
        description="Name of the reviewer"
    )
    review_text: str = Field(
        ...,
        description="Review content text"
    )
    rating: int = Field(
        ...,
        ge=1,
        le=5,
        description="Star rating (1-5)"
    )
    reviewed_at: datetime = Field(
        ...,
        description="When the review was written on the platform"
    )
    created_at: datetime = Field(
        default_factory=utc_now,
        description="When the review was ingested into our system"
    )
    is_reply: Optional[bool] = Field(
        None,
        description="Whether this is a developer reply"
    )
    reply_comment: Optional[str] = Field(
        None,
        description="Developer's reply text"
    )

    @field_validator('platform_type')
    @classmethod
    def validate_platform_type(cls, v: str) -> str:
        """Validate platform_type is one of the allowed values.

        Args:
            v: Platform type string

        Returns:
            str: Validated platform type

        Raises:
            ValueError: If platform type is not APPSTORE or PLAYSTORE
        """
        allowed = {'APPSTORE', 'PLAYSTORE'}
        if v not in allowed:
            raise ValueError(f"Invalid platform_type: {v}. Must be one of {allowed}")
        return v

    @field_validator('rating')
    @classmethod
    def validate_rating(cls, v: int) -> int:
        """Validate rating is between 1 and 5.

        Args:
            v: Rating value

        Returns:
            int: Validated rating

        Raises:
            ValueError: If rating is not between 1 and 5
        """
        if not 1 <= v <= 5:
            raise ValueError(f"Invalid rating: {v}. Must be between 1 and 5")
        return v

    @field_validator('review_text')
    @classmethod
    def validate_review_text_not_empty(cls, v: str) -> str:
        """Validate review_text is not empty.

        Args:
            v: Review text

        Returns:
            str: Validated review text

        Raises:
            ValueError: If review text is empty
        """
        if not v or not v.strip():
            raise ValueError("review_text cannot be empty")
        return v

    model_config = ConfigDict(
        # Allow arbitrary types for datetime
        arbitrary_types_allowed=True,
    )


# Type alias for convenience
AppReview = AppReviewSchema
