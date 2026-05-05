"""schema v4 baseline

Revision ID: 20260430_0001
Revises:
Create Date: 2026-04-30 00:00:00.000000
"""

from __future__ import annotations

from pathlib import Path

from alembic import op


revision = "20260430_0001"
down_revision = None
branch_labels = None
depends_on = None


def _baseline_sql() -> str:
    return (Path(__file__).with_name("20260430_0001_schema_v4_baseline.sql")).read_text()


def upgrade() -> None:
    raw_connection = op.get_bind().connection
    cursor = raw_connection.cursor()
    cursor.execute(_baseline_sql())


def downgrade() -> None:
    raise RuntimeError(
        "schema_v4 baseline downgrade is intentionally unsupported; "
        "reset a local database with scripts/bootstrap_db.py instead"
    )
