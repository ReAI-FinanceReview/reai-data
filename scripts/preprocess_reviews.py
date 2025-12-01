#!/usr/bin/env python3
"""Run preprocess step only."""
import sys
from pathlib import Path

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

from src.pipeline.steps import run_preprocess  # noqa: E402


def main(argv=None) -> int:
    limit = None
    if argv and len(argv) > 0:
        try:
            limit = int(argv[0])
        except ValueError:
            print("limit must be integer")
            return 1
    result = run_preprocess(limit=limit)
    return 0 if result.status == "success" else 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
