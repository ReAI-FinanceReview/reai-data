#!/usr/bin/env python3
"""Bootstrap the local PostgreSQL database for development."""

import sys
from pathlib import Path


ROOT = Path(__file__).parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


from src.bootstrap_db import main  # noqa: E402


if __name__ == "__main__":
    raise SystemExit(main())
