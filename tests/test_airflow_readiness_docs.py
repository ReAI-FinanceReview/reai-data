from pathlib import Path


DOC_PATH = Path(__file__).resolve().parents[1] / "docs" / "airflow-continuous-load-readiness.md"


def test_airflow_readiness_doc_explains_validation_policy_and_follow_up():
    doc = DOC_PATH.read_text(encoding="utf-8")

    required_markers = [
        "## Contract boundary",
        "## Warning vs failure policy",
        "fresh_ingestion",
        "warning",
        "failure",
        "post_aggregate_validate",
        "PYTHONPATH=. uv run python -m src.pipeline.cli",
        "## Follow-up: Metabase/Grafana",
        "Metabase/Grafana readiness metrics dashboard",
    ]
    for marker in required_markers:
        assert marker in doc
