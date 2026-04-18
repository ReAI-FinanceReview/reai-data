from pathlib import Path

import yaml


ROOT = Path(__file__).resolve().parents[1]


def load_compose():
    compose_path = ROOT / "docker-compose.yml"
    assert compose_path.exists(), "docker-compose.yml must exist for local development"
    return yaml.safe_load(compose_path.read_text())


def read_env_template(path: str) -> dict[str, str]:
    env_path = ROOT / path
    assert env_path.exists(), f"{path} must exist for local development"

    values = {}
    for line in env_path.read_text().splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, value = stripped.split("=", 1)
        values[key] = value
    return values


def test_local_dev_compose_declares_postgres_and_minio():
    compose = load_compose()

    assert "postgres" in compose["services"]
    assert "minio" in compose["services"]


def test_postgres_and_minio_ports_are_exposed_for_host_use():
    compose = load_compose()

    assert compose["services"]["postgres"]["ports"] == ["5432:5432"]
    assert compose["services"]["minio"]["ports"] == ["9000:9000", "9001:9001"]


def test_local_env_template_uses_localhost_endpoints():
    env = read_env_template(".env.local.example")

    assert env["DATABASE_URL"] == "postgresql+psycopg2://reai:reai@localhost:5432/reai"
    assert env["MINIO_ENDPOINT"] == "localhost:9000"
    assert env["MINIO_BUCKET"] == "reai-data"


def test_local_docs_explain_compose_startup():
    docs_path = ROOT / "docs" / "local-development.md"
    assert docs_path.exists(), "docs/local-development.md must exist"

    content = docs_path.read_text()
    assert "docker compose up -d" in content
    assert ".env.local.example" in content
    assert "Airflow" in content
