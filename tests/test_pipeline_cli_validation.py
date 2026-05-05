from __future__ import annotations

from datetime import date, datetime, timezone

import pytest
from sqlalchemy import text

from src.pipeline import cli as pipeline_cli


CLI_WARNING_TARGET_DATE = date(2026, 5, 18)
CLI_FAILURE_TARGET_DATE = date(2026, 5, 19)


def _target_datetime(target_date: date) -> datetime:
    return datetime(target_date.year, target_date.month, target_date.day, 9, 30, tzinfo=timezone.utc)


@pytest.mark.requires_db
def test_pipeline_cli_post_aggregate_validate_exits_zero_for_warning_only(
    test_db_url,
    test_db_schema,
    monkeypatch,
):
    monkeypatch.setenv("DATABASE_URL", test_db_url)

    assert (
        pipeline_cli.main(
            [
                "--steps",
                "post_aggregate_validate",
                "--target-date",
                CLI_WARNING_TARGET_DATE.isoformat(),
            ]
        )
        == 0
    )


@pytest.mark.requires_db
def test_pipeline_cli_post_aggregate_validate_exits_non_zero_for_required_failure(
    test_db_engine,
    test_db_url,
    test_db_schema,
    monkeypatch,
):
    storage_path = "s3://reai-data/bronze/post-aggregate/cli-failure.parquet"
    monkeypatch.setenv("DATABASE_URL", test_db_url)
    try:
        with test_db_engine.begin() as connection:
            connection.execute(
                text(
                    """
                    INSERT INTO ingestion_batch (
                        source_type,
                        platform_app_id,
                        app_name,
                        storage_path,
                        record_count,
                        status,
                        created_at,
                        updated_at
                    )
                    VALUES (
                        'APPSTORE',
                        'post.aggregate.cli.failure',
                        'Post Aggregate CLI Failure',
                        :storage_path,
                        1,
                        'PENDING',
                        :created_at,
                        :created_at
                    )
                    """
                ),
                {
                    "storage_path": storage_path,
                    "created_at": _target_datetime(CLI_FAILURE_TARGET_DATE),
                },
            )

        assert (
            pipeline_cli.main(
                [
                    "--steps",
                    "post_aggregate_validate",
                    "--target-date",
                    CLI_FAILURE_TARGET_DATE.isoformat(),
                ]
            )
            == 1
        )
    finally:
        with test_db_engine.begin() as connection:
            connection.execute(
                text("DELETE FROM ingestion_batch WHERE storage_path = :storage_path"),
                {"storage_path": storage_path},
            )
