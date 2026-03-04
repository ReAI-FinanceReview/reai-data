import os
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.pipeline.steps import RunResult, run_preprocess  # noqa: E402


@pytest.mark.integration
@pytest.mark.skipif(not os.getenv("DATABASE_URL"), reason="DATABASE_URL not set for integration test")
def test_preprocess_smoke_with_db_env():
    result = run_preprocess(limit=1)
    assert isinstance(result, RunResult)
