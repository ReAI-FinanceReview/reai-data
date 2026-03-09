#!/usr/bin/env python3
"""Run crawl step only."""
import sys
import os
from dotenv import load_dotenv
from pathlib import Path

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

from src.pipeline.steps import run_crawl  # noqa: E402


def main() -> int:
    result = run_crawl()
    if result.status != "success":
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
