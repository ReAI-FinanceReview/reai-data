"""Local database bootstrap helpers for reproducible development setup."""

from __future__ import annotations

import argparse
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

from alembic import command
from alembic.config import Config
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL, Engine, make_url


ALEMBIC_BASELINE_REVISION = "20260430_0001"

SQL_FILE_ORDER = (
    "schema_v4.sql",
    "app_service_data.sql",
    "apps_data.sql",
    "app_metadata_data.sql",
)

LOCAL_DB_HOSTS = {"localhost", "127.0.0.1", "::1"}


@dataclass(frozen=True)
class BootstrapVerification:
    name: str
    query: str
    expected_count: int


class BootstrapError(RuntimeError):
    """Raised when local DB bootstrap cannot be completed safely."""


def get_project_root() -> Path:
    return Path(__file__).resolve().parents[1]


def get_bootstrap_sql_paths(root: Path) -> list[Path]:
    sql_dir = root / "sql"
    return [sql_dir / filename for filename in SQL_FILE_ORDER]


def build_verification_queries() -> list[BootstrapVerification]:
    return [
        BootstrapVerification("app_service", "SELECT COUNT(*) FROM app_service", 39),
        BootstrapVerification("apps", "SELECT COUNT(*) FROM apps", 63),
        BootstrapVerification(
            "app_metadata_active",
            "SELECT COUNT(*) FROM app_metadata WHERE is_active = TRUE",
            63,
        ),
    ]


def is_local_database_url(url: URL) -> bool:
    return url.host in LOCAL_DB_HOSTS


def load_database_url(root: Path, explicit_url: str | None = None) -> str:
    env_path = root / ".env"
    if env_path.exists():
        load_dotenv(env_path)

    database_url = explicit_url or os.getenv("DATABASE_URL")
    if not database_url:
        raise BootstrapError(
            "DATABASE_URL is required. Create .env from .env.local.example before bootstrapping."
        )
    return database_url


def validate_bootstrap_target(database_url: str) -> URL:
    url = make_url(database_url)
    if not is_local_database_url(url):
        raise BootstrapError(
            f"Refusing to bootstrap non-local database host {url.host!r}. "
            "Use a localhost DATABASE_URL for local development."
        )
    return url


def ensure_sql_files_exist(sql_paths: Iterable[Path]) -> None:
    missing_files = [str(path) for path in sql_paths if not path.exists()]
    if missing_files:
        raise BootstrapError(f"Missing bootstrap SQL files: {', '.join(missing_files)}")


def reset_public_schema(engine: Engine) -> None:
    raw_conn = engine.raw_connection()
    try:
        cursor = raw_conn.cursor()
        cursor.execute("DROP SCHEMA IF EXISTS public CASCADE;")
        cursor.execute("CREATE SCHEMA public;")
        raw_conn.commit()
    finally:
        raw_conn.close()


def execute_sql_file(engine: Engine, sql_path: Path) -> None:
    sql_text = sql_path.read_text()
    raw_conn = engine.raw_connection()
    try:
        cursor = raw_conn.cursor()
        cursor.execute(sql_text)
        raw_conn.commit()
    finally:
        raw_conn.close()


def run_verifications(engine: Engine, verifications: Iterable[BootstrapVerification]) -> None:
    with engine.connect() as conn:
        for verification in verifications:
            actual_count = conn.execute(text(verification.query)).scalar_one()
            if actual_count != verification.expected_count:
                raise BootstrapError(
                    f"Verification failed for {verification.name}: "
                    f"expected {verification.expected_count}, got {actual_count}"
                )


def build_alembic_config(root: Path, database_url: str) -> Config:
    alembic_config = Config(str(root / "alembic.ini"))
    alembic_config.set_main_option("script_location", str(root / "alembic"))
    alembic_config.set_main_option("sqlalchemy.url", database_url)
    alembic_config.attributes["database_url"] = database_url
    return alembic_config


def run_alembic_baseline_and_migrations(
    root: Path,
    database_url: str,
    *,
    stdout=print,
) -> None:
    alembic_config = build_alembic_config(root, database_url)

    stdout(f"- stamping Alembic baseline {ALEMBIC_BASELINE_REVISION}")
    command.stamp(alembic_config, ALEMBIC_BASELINE_REVISION)

    stdout("- applying Alembic migrations to head")
    command.upgrade(alembic_config, "head")


def bootstrap_database(database_url: str | None = None, *, stdout=print) -> None:
    root = get_project_root()
    resolved_url = load_database_url(root, database_url)
    validate_bootstrap_target(resolved_url)

    sql_paths = get_bootstrap_sql_paths(root)
    ensure_sql_files_exist(sql_paths)

    stdout("Bootstrapping local database")
    stdout(f"- target: {make_url(resolved_url).render_as_string(hide_password=True)}")

    engine = create_engine(resolved_url)
    try:
        stdout("- resetting public schema")
        reset_public_schema(engine)

        for sql_path in sql_paths:
            stdout(f"- applying {sql_path.name}")
            execute_sql_file(engine, sql_path)

        run_alembic_baseline_and_migrations(root, resolved_url, stdout=stdout)

        stdout("- running verification queries")
        run_verifications(engine, build_verification_queries())
        stdout("Bootstrap complete")
    finally:
        engine.dispose()


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Reset the local PostgreSQL public schema and load required REAI seed data."
    )
    parser.add_argument(
        "--database-url",
        default=None,
        help="Override DATABASE_URL. Intended for local localhost targets only.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_arg_parser()
    args = parser.parse_args(argv)

    try:
        bootstrap_database(args.database_url)
    except BootstrapError as exc:
        print(f"Bootstrap failed: {exc}")
        return 1

    return 0
