#!/usr/bin/env python3
"""Unified pipeline entrypoint."""
import sys
from pathlib import Path

# Ensure project root on path
ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

from src.pipeline.cli import main  # noqa: E402


if __name__ == "__main__":
    raise SystemExit(main())
