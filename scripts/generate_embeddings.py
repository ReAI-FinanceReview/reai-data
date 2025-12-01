#!/usr/bin/env python3
"""Run embedding generation step only."""
import sys
from pathlib import Path

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

from src.pipeline.steps import run_generate_embeddings  # noqa: E402


def main(argv=None) -> int:
    limit = None
    model_name = "text-embedding-3-small"
    if argv and len(argv) > 0:
        try:
            limit = int(argv[0])
        except ValueError:
            print("limit must be integer")
            return 1
    if argv and len(argv) > 1:
        model_name = argv[1]
    result = run_generate_embeddings(limit=limit, model_name=model_name)
    return 0 if result.status == "success" else 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
