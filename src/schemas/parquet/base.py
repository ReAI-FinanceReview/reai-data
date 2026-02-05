"""Base utilities for Parquet schemas.

This module provides common utilities for Parquet schema definitions,
including UUID v7 generation and timestamp utilities.
"""

import uuid6
from datetime import datetime, timezone


def generate_uuid_v7() -> str:
    """Generate UUID v7 (time-sortable UUID).

    UUID v7 is designed to be time-sortable and provides better database indexing
    performance compared to UUID v4.

    Note:
        Python's standard uuid module doesn't support v7 yet (as of Python 3.11).
        This implementation uses uuid1 as a temporary workaround, which provides
        time-based ordering but uses MAC address.

        For production, consider:
        1. Using the uuid6 library: `uuid6.uuid7()`
        2. Using PostgreSQL's gen_random_uuid() via database defaults
        3. Implementing custom UUID v7 generation based on RFC 9562

    Returns:
        str: UUID v7 string representation

    Example:
        >>> review_id = generate_uuid_v7()
        >>> len(review_id)
        36
    """
    # TODO: Replace with proper UUID v7 implementation
    # Option 1: pip install uuid6; return str(uuid6.uuid7())
    # Option 2: Use PostgreSQL gen_random_uuid() via server_default
    return str(uuid6.uuid7())


def utc_now() -> datetime:
    """Get current UTC timestamp with timezone info.

    Returns:
        datetime: Current UTC time as timezone-aware datetime

    Example:
        >>> now = utc_now()
        >>> now.tzinfo is not None
        True
    """
    return datetime.now(timezone.utc)


def to_utc(dt: datetime) -> datetime:
    """Convert datetime to UTC timezone.

    Args:
        dt: Input datetime (naive or aware)

    Returns:
        datetime: UTC datetime with timezone info

    Example:
        >>> from datetime import datetime
        >>> naive_dt = datetime(2026, 1, 1, 12, 0)
        >>> utc_dt = to_utc(naive_dt)
        >>> utc_dt.tzinfo is not None
        True
    """
    if dt.tzinfo is None:
        # Assume naive datetime is UTC
        return dt.replace(tzinfo=timezone.utc)
    else:
        # Convert aware datetime to UTC
        return dt.astimezone(timezone.utc)
