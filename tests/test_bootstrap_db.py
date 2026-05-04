from pathlib import Path

import pytest
from psycopg2 import OperationalError as PsycopgOperationalError
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import OperationalError

from src import bootstrap_db as bootstrap_module
from src.bootstrap_db import (
    BootstrapError,
    BootstrapVerification,
    build_verification_queries,
    get_bootstrap_sql_paths,
    is_local_database_url,
    validate_bootstrap_target,
)


ROOT = Path(__file__).resolve().parents[1]


def _schema_sql_for_test() -> str:
    sql_content = (ROOT / "sql" / "schema_v4.sql").read_text()
    filtered_lines = []
    for line in sql_content.splitlines():
        code = line.split("--")[0]
        if "uuid-ossp" in code:
            continue
        if "uuid_generate_v4()" in code:
            line = line.replace("DEFAULT uuid_generate_v4()", "")
        if "ltree" in code and "CREATE EXTENSION" in code:
            continue
        if "USING GIST" in code and "org_id" in code:
            continue
        if "ltree" in code and "COMMENT" not in code:
            line = line.replace("ltree", "TEXT")
        filtered_lines.append(line)
    return "\n".join(filtered_lines)


def _reset_schema(test_db_engine) -> None:
    raw_conn = test_db_engine.raw_connection()
    try:
        cursor = raw_conn.cursor()
        cursor.execute("DROP SCHEMA IF EXISTS public CASCADE;")
        cursor.execute("CREATE SCHEMA public;")
        cursor.execute(_schema_sql_for_test())
        raw_conn.commit()
    finally:
        raw_conn.close()


def _execute_sql_file(test_db_engine, sql_path: Path) -> None:
    raw_conn = test_db_engine.raw_connection()
    try:
        cursor = raw_conn.cursor()
        cursor.execute(sql_path.read_text())
        raw_conn.commit()
    finally:
        raw_conn.close()


def test_bootstrap_sql_paths_are_in_required_order():
    sql_paths = get_bootstrap_sql_paths(ROOT)

    assert [path.name for path in sql_paths] == [
        "schema_v4.sql",
        "app_service_data.sql",
        "apps_data.sql",
        "app_metadata_data.sql",
    ]


def test_is_local_database_url_accepts_localhost_targets():
    assert is_local_database_url(make_url("postgresql://reai:reai@localhost:5432/reai"))
    assert is_local_database_url(make_url("postgresql://reai:reai@127.0.0.1:5432/reai"))
    assert is_local_database_url(make_url("postgresql://reai:reai@[::1]:5432/reai"))


def test_is_local_database_url_rejects_remote_targets():
    assert not is_local_database_url(make_url("postgresql://reai:reai@db.internal:5432/reai"))
    assert not is_local_database_url(make_url("postgresql://reai:reai@10.0.0.15:5432/reai"))


def test_validate_bootstrap_target_accepts_localhost_url():
    validated = validate_bootstrap_target("postgresql://reai:reai@localhost:5432/reai")

    assert validated.host == "localhost"


def test_validate_bootstrap_target_rejects_remote_url():
    with pytest.raises(BootstrapError):
        validate_bootstrap_target("postgresql://reai:reai@db.internal:5432/reai")


def test_build_verification_queries_check_required_seed_counts():
    queries = build_verification_queries()

    assert queries == [
        BootstrapVerification("app_service", "SELECT COUNT(*) FROM app_service", 39),
        BootstrapVerification("apps", "SELECT COUNT(*) FROM apps", 63),
        BootstrapVerification("app_metadata_active", "SELECT COUNT(*) FROM app_metadata WHERE is_active = TRUE", 63),
    ]


def test_required_seed_files_define_idempotent_conflict_handlers():
    assert "ON CONFLICT (service_id) DO UPDATE" in (ROOT / "sql" / "app_service_data.sql").read_text()
    assert "ON CONFLICT (app_id) DO UPDATE" in (ROOT / "sql" / "apps_data.sql").read_text()
    assert "UNIQUE (app_id, valid_from)" in (ROOT / "sql" / "schema_v4.sql").read_text()
    assert "ON CONFLICT (app_id, valid_from) DO UPDATE" in (
        ROOT / "sql" / "app_metadata_data.sql"
    ).read_text()


def test_required_seed_files_are_reexecution_safe(test_db_engine):
    seed_paths = get_bootstrap_sql_paths(ROOT)[1:]

    try:
        _reset_schema(test_db_engine)
    except (OperationalError, PsycopgOperationalError) as exc:
        pytest.skip(f"test PostgreSQL is not available: {exc}")

    try:
        for _ in range(2):
            for sql_path in seed_paths:
                _execute_sql_file(test_db_engine, sql_path)

        with test_db_engine.connect() as conn:
            counts = {
                "app_service": conn.execute(text("SELECT COUNT(*) FROM app_service")).scalar_one(),
                "apps": conn.execute(text("SELECT COUNT(*) FROM apps")).scalar_one(),
                "app_metadata": conn.execute(
                    text("SELECT COUNT(*) FROM app_metadata WHERE is_active = TRUE")
                ).scalar_one(),
            }

        assert counts == {
            "app_service": 39,
            "apps": 63,
            "app_metadata": 63,
        }
    finally:
        _reset_schema(test_db_engine)


def test_bootstrap_script_exists():
    bootstrap_script = ROOT / "scripts" / "bootstrap_db.py"
    assert bootstrap_script.exists(), "scripts/bootstrap_db.py must exist for local bootstrap"


def test_local_docs_reference_bootstrap_command():
    docs_path = ROOT / "docs" / "local-development.md"
    content = docs_path.read_text()

    assert "scripts/bootstrap_db.py" in content
    assert "app_metadata_data.sql" in content
    assert "crawl_reviews.py" in content
    assert content.index("applies migrations to `head`") < content.index("loads required reference seed data")


def test_build_alembic_config_points_at_project_script_location():
    database_url = "postgresql+psycopg2://reai:reai@localhost:5432/reai"

    config = bootstrap_module.build_alembic_config(ROOT, database_url)

    assert config.config_file_name == str(ROOT / "alembic.ini")
    assert config.get_main_option("script_location") == str(ROOT / "alembic")
    assert config.get_main_option("sqlalchemy.url") == database_url
    assert config.attributes["database_url"] == database_url


def test_bootstrap_explicit_database_url_controls_alembic_when_env_differs(
    monkeypatch,
    test_db_engine,
    test_db_url,
):
    if not is_local_database_url(make_url(test_db_url)):
        pytest.skip("bootstrap safety test requires a local PostgreSQL target")

    monkeypatch.setenv(
        "DATABASE_URL",
        "postgresql+psycopg2://reai:reai@localhost:1/should_not_be_used_by_alembic",
    )

    try:
        bootstrap_module.bootstrap_database(test_db_url, stdout=lambda message: None)
        with test_db_engine.connect() as conn:
            assert conn.execute(text("SELECT version_num FROM alembic_version")).scalar_one() == (
                bootstrap_module.ALEMBIC_BASELINE_REVISION
            )
            assert conn.execute(text("SELECT COUNT(*) FROM app_service")).scalar_one() == 39
    except (OperationalError, PsycopgOperationalError) as exc:
        pytest.skip(f"test PostgreSQL is not available: {exc}")
    finally:
        try:
            _reset_schema(test_db_engine)
        except (OperationalError, PsycopgOperationalError):
            pass


def test_bootstrap_runs_migrations_before_seed_sql(monkeypatch):
    calls = []

    class FakeEngine:
        def dispose(self):
            calls.append("dispose")

    database_url = "postgresql+psycopg2://reai:reai@localhost:5432/reai"

    monkeypatch.setattr(bootstrap_module, "get_project_root", lambda: ROOT)
    monkeypatch.setattr(
        bootstrap_module,
        "load_database_url",
        lambda root, explicit_url=None: database_url,
    )
    monkeypatch.setattr(bootstrap_module, "validate_bootstrap_target", lambda url: make_url(url))
    monkeypatch.setattr(
        bootstrap_module,
        "ensure_sql_files_exist",
        lambda sql_paths: calls.append("ensure_sql_files_exist"),
    )
    monkeypatch.setattr(bootstrap_module, "create_engine", lambda url: FakeEngine())
    monkeypatch.setattr(bootstrap_module, "reset_public_schema", lambda engine: calls.append("reset"))
    monkeypatch.setattr(
        bootstrap_module,
        "execute_sql_file",
        lambda engine, sql_path: calls.append(sql_path.name),
    )
    monkeypatch.setattr(
        bootstrap_module,
        "run_alembic_baseline_and_migrations",
        lambda root, database_url, stdout=print: calls.append("alembic"),
    )
    monkeypatch.setattr(
        bootstrap_module,
        "run_verifications",
        lambda engine, verifications: calls.append("verify"),
    )

    bootstrap_module.bootstrap_database(stdout=lambda message: None)

    assert calls == [
        "ensure_sql_files_exist",
        "reset",
        "schema_v4.sql",
        "alembic",
        "app_service_data.sql",
        "apps_data.sql",
        "app_metadata_data.sql",
        "verify",
        "dispose",
    ]
