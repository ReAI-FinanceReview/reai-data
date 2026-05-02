from pathlib import Path
import tomllib


ROOT = Path(__file__).resolve().parents[1]


def test_alembic_dependency_is_declared():
    pyproject = tomllib.loads((ROOT / "pyproject.toml").read_text())

    dependencies = pyproject["project"]["dependencies"]

    assert any(dependency.startswith("alembic") for dependency in dependencies)
