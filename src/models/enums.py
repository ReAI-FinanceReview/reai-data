"""
Central ENUM definitions for the application.

This module consolidates all ENUM types to avoid duplication across multiple files.
All models should import ENUMs from this module instead of defining their own.
"""

import enum


class PlatformType(enum.Enum):
    """Platform type for app reviews."""
    APPSTORE = "APPSTORE"
    PLAYSTORE = "PLAYSTORE"


class AppType(enum.Enum):
    """Application type classification."""
    CONSUMER = "CONSUMER"
    CORPORATE = "CORPORATE"
    GLOBAL = "GLOBAL"


class ProcessingStatusType(enum.Enum):
    """Processing status for reviews in the pipeline.

    SKIPPED is terminal: rule-based ABSA cannot analyze the review (empty text, no Hangul,
    no aspect). It is not retried and, like FAILED, never reaches the marts (issue #94).
    """
    RAW = "RAW"
    CLEANED = "CLEANED"
    ANALYZED = "ANALYZED"
    FAILED = "FAILED"
    SKIPPED = "SKIPPED"


class AnalysisStatusType(enum.Enum):
    """Status of LLM analysis tasks."""
    PENDING = "PENDING"
    PROCESSING = "PROCESSING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"


class SentimentType(enum.Enum):
    """Sentiment classification."""
    POSITIVE = "POSITIVE"
    NEGATIVE = "NEGATIVE"
    NEUTRAL = "NEUTRAL"


class IngestionBatchStatusType(enum.Enum):
    """Status of Parquet batch ingestion (DLQ management)."""
    PENDING = "PENDING"
    LOADED = "LOADED"
    FAILED = "FAILED"
    RETRYING = "RETRYING"
    DEAD_LETTER = "DEAD_LETTER"


class CategoryType(enum.Enum):
    """5-dimensional review analysis category (오방성)."""
    USABILITY = "USABILITY"
    STABILITY = "STABILITY"
    DESIGN = "DESIGN"
    CUSTOMER_SUPPORT = "CUSTOMER_SUPPORT"
    SPEED = "SPEED"
