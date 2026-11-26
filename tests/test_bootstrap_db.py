import logging
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml
from psycopg2 import OperationalError as PsycopgOperationalError
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import OperationalError

from tests.conftest import _apply_migrations

from src import bootstrap_db as bootstrap_module
from src.crawlers.base_crawler import BaseCrawler
from src.bootstrap_db import (
    BootstrapError,
    BootstrapVerification,
    build_verification_queries,
    get_bootstrap_sql_paths,
    is_local_database_url,
    validate_bootstrap_target,
    validate_published_port,
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

    # schema_v4.sql 은 베이스라인 스냅샷이므로 이후 리비전의 컬럼/제약이 없다.
    # 세션 스코프 스키마를 여기서 되돌리면서 마이그레이션을 빠뜨리면, 이 파일
    # 이후에 실행되는 테스트가 그 컬럼을 못 찾는다.
    _apply_migrations(test_db_engine.url.render_as_string(hide_password=False))


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


def test_validate_published_port_accepts_matching_or_unset_port():
    url = make_url("postgresql://reai:reai@localhost:5434/reai")

    validate_published_port(url, "5434")
    validate_published_port(url, " 05434 ")
    validate_published_port(url, None)


def test_validate_published_port_reads_a_portless_url_as_the_libpq_default():
    url = make_url("postgresql://reai:reai@localhost/reai")

    validate_published_port(url, str(bootstrap_module.POSTGRES_DEFAULT_PORT))
    with pytest.raises(BootstrapError, match="POSTGRES_PORT"):
        validate_published_port(url, str(bootstrap_module.POSTGRES_DEFAULT_PORT + 2))


def test_validate_published_port_rejects_a_port_compose_does_not_publish():
    url = make_url("postgresql://reai:reai@localhost:5432/reai")

    with pytest.raises(BootstrapError, match="--database-url"):
        validate_published_port(url, "5434")
    with pytest.raises(BootstrapError, match="not a port number"):
        validate_published_port(url, "127.0.0.1:5432")


def _isolate_env(monkeypatch, *names: str) -> None:
    # load_dotenv() writes os.environ directly. Registering each name with
    # monkeypatch first makes teardown remove whatever the test loads.
    for name in names:
        monkeypatch.setenv(name, "")
        monkeypatch.delenv(name)


@pytest.fixture(autouse=True)
def _no_inherited_database_env(monkeypatch):
    # bootstrap_database() loads the repository .env into os.environ, and the
    # published-port check reads POSTGRES_PORT from there. Without this, one
    # test's load (or a developer's shell export) decides whether a later test
    # is refused -- green in CI, red for anyone who moved the port.
    _isolate_env(monkeypatch, "POSTGRES_PORT", "DATABASE_URL")


def _env_with_port_appended(tmp_path: Path) -> Path:
    """The template's POSTGRES_PORT/DATABASE_URL lines plus a second POSTGRES_PORT
    appended at the end -- what Compose publishes with, after DATABASE_URL has
    already resolved the first one."""
    template_lines = (ROOT / ".env.local.example").read_text().splitlines()
    kept = [line for line in template_lines if line.startswith(("POSTGRES_PORT=", "DATABASE_URL="))]
    template_port = next(line for line in kept if line.startswith("POSTGRES_PORT=")).split("=", 1)[1]
    moved = str(int(template_port) + 2)
    env_path = tmp_path / ".env"
    env_path.write_text("\n".join([*kept, f"POSTGRES_PORT={moved}"]) + "\n")
    return env_path


@pytest.mark.parametrize("database_url", [None, ""])
def test_bootstrap_refuses_env_whose_url_port_drifted_from_postgres_port(
    monkeypatch, tmp_path, database_url
):
    _env_with_port_appended(tmp_path)
    # tmp_path has no sql/ directory, so even a missed refusal stops at the SQL
    # file check, before any engine is created.
    monkeypatch.setattr(bootstrap_module, "get_project_root", lambda: tmp_path)

    # "" is checked too: load_database_url() falls back to .env for it.
    with pytest.raises(BootstrapError, match="POSTGRES_PORT"):
        bootstrap_module.bootstrap_database(database_url, stdout=lambda message: None)


def test_bootstrap_explicit_database_url_skips_the_published_port_check(monkeypatch, tmp_path):
    _env_with_port_appended(tmp_path)
    monkeypatch.setattr(bootstrap_module, "get_project_root", lambda: tmp_path)

    # The explicit URL is the caller's stated target (CI, the test database), so
    # the run gets past the port check and stops at the missing SQL files instead.
    with pytest.raises(BootstrapError, match="Missing bootstrap SQL files"):
        bootstrap_module.bootstrap_database(
            "postgresql+psycopg2://reai:reai@localhost:5433/reai",
            stdout=lambda message: None,
        )


def test_build_verification_queries_check_required_seed_counts():
    queries = build_verification_queries()

    assert queries == [
        BootstrapVerification("app_service", "SELECT COUNT(*) FROM app_service", 39),
        BootstrapVerification("apps", "SELECT COUNT(*) FROM apps", 65),
        BootstrapVerification("app_metadata_active", "SELECT COUNT(*) FROM app_metadata WHERE is_active = TRUE", 61),
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
                "app_metadata": conn.execute(text("SELECT COUNT(*) FROM app_metadata")).scalar_one(),
                "app_metadata_active": conn.execute(
                    text("SELECT COUNT(*) FROM app_metadata WHERE is_active = TRUE")
                ).scalar_one(),
            }

        assert counts == {
            "app_service": 39,
            "apps": 65,
            "app_metadata": 65,
            "app_metadata_active": 61,
        }
    finally:
        _reset_schema(test_db_engine)


def _crawl_ids_by_platform() -> set[tuple[str, str]]:
    # Parse the id files exactly as the crawlers do (BaseCrawler.read_app_ids), at the
    # paths the crawler config names, so this cannot drift from what actually gets crawled.
    config = yaml.safe_load((ROOT / "config" / "crawler_config.yml").read_text(encoding="utf-8"))
    reader = SimpleNamespace(logger=logging.getLogger(__name__))
    return {
        (platform, app_id)
        for platform, key in (("APPSTORE", "appstore"), ("PLAYSTORE", "playstore"))
        for app_id in BaseCrawler.read_app_ids(reader, str(ROOT / config["app_ids"][key]))
    }


def test_seeded_open_links_match_the_crawl_id_files(test_db_engine):
    # The crawl lists and the seed catalog are kept in step by hand until the catalog
    # becomes the single source (#119). Pin the invariants a hand edit can break: every
    # crawled id has exactly one open link, every open link is crawled, is_active agrees
    # with valid_to, and no app has two links valid on the same day.
    seed_paths = get_bootstrap_sql_paths(ROOT)[1:]
    crawl_ids = _crawl_ids_by_platform()

    try:
        _reset_schema(test_db_engine)
    except (OperationalError, PsycopgOperationalError) as exc:
        pytest.skip(f"test PostgreSQL is not available: {exc}")

    try:
        for sql_path in seed_paths:
            _execute_sql_file(test_db_engine, sql_path)

        with test_db_engine.connect() as conn:
            open_links = conn.execute(
                text(
                    "SELECT a.platform_type::text, a.platform_app_id "
                    "FROM app_metadata m JOIN apps a USING (app_id) "
                    "WHERE m.valid_to IS NULL"
                )
            ).all()
            flag_mismatches = conn.execute(
                text(
                    "SELECT COUNT(*) FROM app_metadata "
                    "WHERE is_active IS DISTINCT FROM (valid_to IS NULL)"
                )
            ).scalar_one()
            overlapping_pairs = conn.execute(
                text(
                    "SELECT COUNT(*) FROM app_metadata m1 JOIN app_metadata m2 "
                    "ON m1.app_id = m2.app_id AND m1.id < m2.id "
                    "AND daterange(m1.valid_from, m1.valid_to, '[]') "
                    "&& daterange(m2.valid_from, m2.valid_to, '[]')"
                )
            ).scalar_one()

        assert len(open_links) == len(set(open_links)), "an id has more than one open link"
        assert set(open_links) == crawl_ids
        assert flag_mismatches == 0
        assert overlapping_pairs == 0
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
            assert conn.execute(text("SELECT version_num FROM alembic_version")).scalar_one()
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
