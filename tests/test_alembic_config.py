from pathlib import Path
import configparser
import tomllib


ROOT = Path(__file__).resolve().parents[1]
BASELINE_REVISION = "20260430_0001"


def test_alembic_dependency_is_declared():
    pyproject = tomllib.loads((ROOT / "pyproject.toml").read_text())

    dependencies = pyproject["project"]["dependencies"]

    assert any(dependency.startswith("alembic") for dependency in dependencies)


def test_alembic_ini_points_to_local_script_directory():
    config = configparser.ConfigParser()
    config.read(ROOT / "alembic.ini")

    assert config["alembic"]["script_location"] == "alembic"
    assert config["alembic"]["prepend_sys_path"] == "."


def test_alembic_env_imports_project_metadata():
    env_py = (ROOT / "alembic" / "env.py").read_text()

    assert "from src.models import Base" in env_py
    assert "target_metadata = Base.metadata" in env_py
    assert "compare_type=True" in env_py
    assert "compare_server_default=True" in env_py


def test_baseline_revision_file_exists_and_is_head():
    revision_path = ROOT / "alembic" / "versions" / "20260430_0001_schema_v4_baseline.py"
    content = revision_path.read_text()

    assert f'revision = "{BASELINE_REVISION}"' in content
    assert "down_revision = None" in content
    assert "20260430_0001_schema_v4_baseline.sql" in content


def test_baseline_sql_snapshot_matches_schema_v4_at_adoption():
    baseline_sql = ROOT / "alembic" / "versions" / "20260430_0001_schema_v4_baseline.sql"
    schema_v4 = ROOT / "sql" / "schema_v4.sql"

    assert baseline_sql.read_text() == schema_v4.read_text()


def test_schema_management_docs_define_migration_workflow_and_seed_ownership():
    content = (ROOT / "docs" / "schema-management.md").read_text()

    assert "sql/schema_v4.sql is the immutable Alembic baseline snapshot" in content
    assert "uv run alembic stamp 20260430_0001" in content
    assert "uv run alembic revision --autogenerate -m" in content
    assert "uv run alembic upgrade head" in content
    assert "Required business reference rows remain in seed SQL" in content
