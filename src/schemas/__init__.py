"""Pydantic schemas for data validation.

This package contains Pydantic schemas for validating data structures,
especially for Parquet files stored on NAS.
"""

from .parquet import AppReviewSchema, ReviewPreprocessedSchema

__all__ = [
    'AppReviewSchema',
    'ReviewPreprocessedSchema',
]
