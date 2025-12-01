#!/usr/bin/env python3
"""Run embedding generation step only."""
import sys
from pathlib import Path

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

from src.pipeline.steps import run_generate_embeddings  # noqa: E402


def main(argv=None) -> int:
    """
    Run the embedding generation step with optional command-line-like arguments.
    
    Parameters:
        argv (list[str] | None): Optional list of arguments where the first element, if present, is parsed as an integer limit for items to process, and the second element, if present, is the embedding model name to use.
    
    Returns:
        int: Exit status code — `0` if embedding generation completed with status "success", `1` otherwise (including when `argv[0]` cannot be parsed as an integer).
    """
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