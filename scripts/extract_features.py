#!/usr/bin/env python3
"""Run feature extraction step only."""
import sys
from pathlib import Path

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

from src.pipeline.steps import run_extract_features  # noqa: E402


def main(argv=None) -> int:
    """
    Entry point that runs the feature extraction step using an optional numeric limit.
    
    Parameters:
        argv (list[str] | None): Command-line arguments; if provided and non-empty, the first element is parsed as an integer and used as the extraction `limit`. If parsing fails, an error message is printed and the function returns an error exit code.
    
    Returns:
        int: Exit code — `0` if feature extraction completed with status "success", `1` on failure or if the provided limit is not a valid integer.
    """
    limit = None
    if argv and len(argv) > 0:
        try:
            limit = int(argv[0])
        except ValueError:
            print("limit must be integer")
            return 1
    result = run_extract_features(limit=limit)
    return 0 if result.status == "success" else 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))