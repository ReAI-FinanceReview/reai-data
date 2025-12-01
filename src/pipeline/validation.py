"""Lightweight validation helpers for pipeline steps."""
from typing import Dict, Optional


def make_count_validation(input_count: Optional[int], output_count: Optional[int]) -> Dict[str, int]:
    """
    Create a validation dictionary summarizing input and output row counts and their difference.
    
    Parameters:
        input_count (Optional[int]): Input row count; treated as 0 when `None`.
        output_count (Optional[int]): Output row count; treated as 0 when `None`.
    
    Returns:
        Dict[str, int]: Dictionary with keys:
            - "input_count": input count with `None` treated as 0.
            - "output_count": output count with `None` treated as 0.
            - "delta": `output_count - input_count` using the adjusted values.
    """
    return {
        "input_count": input_count or 0,
        "output_count": output_count or 0,
        "delta": (output_count or 0) - (input_count or 0),
    }