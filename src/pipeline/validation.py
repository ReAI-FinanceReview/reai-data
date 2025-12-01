"""Lightweight validation helpers for pipeline steps."""
from typing import Dict, Optional


def make_count_validation(input_count: Optional[int], output_count: Optional[int]) -> Dict[str, int]:
    """Return simple validation metrics for row counts."""
    return {
        "input_count": input_count or 0,
        "output_count": output_count or 0,
        "delta": (output_count or 0) - (input_count or 0),
    }
