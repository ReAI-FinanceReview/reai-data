import re
from pathlib import Path

import yaml


ROOT = Path(__file__).resolve().parents[1]


def load_compose():
    compose_path = ROOT / "docker-compose.yml"
    assert compose_path.exists(), "docker-compose.yml must exist for local development"
    return yaml.safe_load(compose_path.read_text())


# Both of Compose's default forms, which are also the two spellings python-dotenv
# resolves in the env template. Searching rather than matching the whole host side
# keeps the bind-address form (127.0.0.1:${POSTGRES_PORT:-5432}:5432) valid.
POSTGRES_PORT_DEFAULT = re.compile(r"\$\{POSTGRES_PORT:?-(\d+)\}")


def compose_postgres_host_port_default() -> str:
    """Read the default host port from the compose postgres port mapping.

    Returns "5432" for ``${POSTGRES_PORT:-5432}:5432`` and for the
    bind-address form ``127.0.0.1:${POSTGRES_PORT:-5432}:5432``.
    """
    compose = load_compose()
    mapping = compose["services"]["postgres"]["ports"][0]
    host_side = mapping.rsplit(":", 1)[0]

    match = POSTGRES_PORT_DEFAULT.search(host_side)
    assert match, (
        "postgres must publish through a POSTGRES_PORT default so the host port "
        f"can be moved off 5432: {mapping!r}"
    )
    return match.group(1)


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

    # Deliberately not an exact-string match: overridability and the default are
    # asserted by compose_postgres_host_port_default(), so a later hardening edit
    # such as binding to 127.0.0.1 does not have to fight this line.
    postgres_ports = compose["services"]["postgres"]["ports"]
    assert len(postgres_ports) == 1
    assert postgres_ports[0].endswith(":5432")
    assert compose_postgres_host_port_default() == "5432"
    assert compose["services"]["minio"]["ports"] == ["9000:9000", "9001:9001"]


def test_minio_images_use_pinned_official_release_tags():
    compose = load_compose()

    # This pins local development to reviewed release tags.
    assert compose["services"]["minio"]["image"] == "minio/minio:RELEASE.2025-09-07T16-13-09Z"
    assert compose["services"]["minio-init"]["image"] == "minio/mc:RELEASE.2025-08-13T08-35-41Z"


def test_local_env_template_uses_localhost_endpoints():
    env = read_env_template(".env.local.example")

    # The port is not pinned here; test_local_env_template_derives_its_database_port
    # owns that contract so it can fail on its own.
    assert env["DATABASE_URL"].startswith("postgresql+psycopg2://reai:reai@localhost:")
    assert env["DATABASE_URL"].endswith("/reai")
    assert env["MINIO_ENDPOINT"] == "localhost:9000"
    assert env["MINIO_BUCKET"] == "reai-data"


def test_local_env_template_derives_its_database_port():
    """The template must take its port from POSTGRES_PORT rather than hardcode one.

    A hardcoded port aims host-side tools at whatever already occupies it, and
    bootstrap_db.py opens with DROP SCHEMA public CASCADE. python-dotenv resolves
    the same ${VAR:-default} form Compose publishes with, so one variable moves
    the container and the connection string together.
    """
    env = read_env_template(".env.local.example")

    match = POSTGRES_PORT_DEFAULT.search(env["DATABASE_URL"])
    assert match, (
        "DATABASE_URL must take its port from POSTGRES_PORT, not a literal: "
        f"{env['DATABASE_URL']!r}"
    )
    assert match.group(1) == compose_postgres_host_port_default()


def test_local_docs_explain_compose_startup():
    docs_path = ROOT / "docs" / "local-development.md"
    assert docs_path.exists(), "docs/local-development.md must exist"

    content = docs_path.read_text()
    assert "docker compose up -d" in content
    assert ".env.local.example" in content
    assert "Airflow" in content
    assert "docs/schema-management.md" in content
