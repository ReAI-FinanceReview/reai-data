from pathlib import Path

import yaml


ROOT = Path(__file__).resolve().parents[1]


def load_bootstrap_workflow():
    workflow_path = ROOT / ".github" / "workflows" / "bootstrap-db.yml"
    assert workflow_path.exists(), "CI bootstrap workflow must exist"
    return yaml.safe_load(workflow_path.read_text())


def test_ci_bootstrap_workflow_runs_against_pgvector_postgres():
    workflow = load_bootstrap_workflow()

    job = workflow["jobs"]["bootstrap-db"]
    postgres = job["services"]["postgres"]

    assert "pull_request" in workflow["on"]
    assert workflow["on"]["push"]["branches"] == ["main"]
    assert postgres["image"] == "pgvector/pgvector:pg17"
    assert postgres["ports"] == ["5432:5432"]
    assert postgres["env"] == {
        "POSTGRES_DB": "reai",
        "POSTGRES_USER": "reai",
        "POSTGRES_PASSWORD": "reai",
        "POSTGRES_INITDB_ARGS": "-E UTF8 --locale=C",
    }
    assert "pg_isready -U reai -d reai" in postgres["options"]


def test_ci_bootstrap_workflow_documents_reproducible_commands():
    workflow = load_bootstrap_workflow()

    job = workflow["jobs"]["bootstrap-db"]
    uv_step = next(step for step in job["steps"] if step["name"] == "Set up uv")
    run_commands = "\n".join(
        step["run"]
        for step in job["steps"]
        if "run" in step
    )

    assert uv_step["uses"] == "astral-sh/setup-uv@v8.1.0"
    assert (
        "PYTHONPATH=. uv run python scripts/bootstrap_db.py "
        "--database-url postgresql+psycopg2://reai:reai@localhost:5432/reai"
    ) in run_commands
    assert (
        "PYTHONPATH=. uv run pytest "
        "tests/test_bootstrap_db.py tests/test_local_dev_setup.py"
    ) in run_commands
