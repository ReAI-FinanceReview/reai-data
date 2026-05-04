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
    assert "workflow_dispatch" in workflow["on"]
    assert workflow["on"]["push"]["branches"] == ["main"]
    assert (
        postgres["image"]
        == "pgvector/pgvector:pg17@sha256:494dff7e67e7bc2c826b94c331364978d145ebb86fd338154138b084223b7f67"
    )
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
    checkout_step = next(step for step in job["steps"] if step["name"] == "Check out repository")
    python_step = next(step for step in job["steps"] if step["name"] == "Set up Python")
    uv_step = next(step for step in job["steps"] if step["name"] == "Set up uv")
    run_commands = "\n".join(
        step["run"]
        for step in job["steps"]
        if "run" in step
    )

    assert checkout_step["uses"] == "actions/checkout@de0fac2e4500dabe0009e67214ff5f5447ce83dd"
    assert python_step["uses"] == "actions/setup-python@a309ff8b426b58ec0e2a45f0f869d46889d02405"
    assert uv_step["uses"] == "astral-sh/setup-uv@08807647e7069bb48b6ef5acd8ec9567f424441b"
    assert (
        "PYTHONPATH=. uv run python scripts/bootstrap_db.py "
        "--database-url postgresql+psycopg2://reai:reai@localhost:5432/reai"
    ) in run_commands
    assert (
        "PYTHONPATH=. uv run pytest "
        "tests/test_ci_workflows.py tests/test_bootstrap_db.py tests/test_local_dev_setup.py"
    ) in run_commands
    assert "PYTHONPATH=. uv run alembic heads" in run_commands
    assert "PYTHONPATH=. uv run alembic current --check-heads" in run_commands
    assert (
        "PYTHONPATH=. uv run pytest "
        "tests/test_ci_workflows.py tests/test_bootstrap_db.py "
        "tests/test_local_dev_setup.py tests/test_alembic_config.py"
    ) in run_commands
