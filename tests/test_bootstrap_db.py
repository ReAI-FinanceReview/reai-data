from pathlib import Path

import pytest
from sqlalchemy.engine import make_url

from src.bootstrap_db import (
    BootstrapError,
    BootstrapVerification,
    build_verification_queries,
    get_bootstrap_sql_paths,
    is_local_database_url,
    validate_bootstrap_target,
)


ROOT = Path(__file__).resolve().parents[1]


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


def test_bootstrap_script_exists():
    bootstrap_script = ROOT / "scripts" / "bootstrap_db.py"
    assert bootstrap_script.exists(), "scripts/bootstrap_db.py must exist for local bootstrap"


def test_local_docs_reference_bootstrap_command():
    docs_path = ROOT / "docs" / "local-development.md"
    content = docs_path.read_text()

    assert "scripts/bootstrap_db.py" in content
    assert "app_metadata_data.sql" in content
    assert "crawl_reviews.py" in content
