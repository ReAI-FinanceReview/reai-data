from __future__ import annotations

import re
from datetime import date, datetime, timezone

import pytest
from sqlalchemy import text
from uuid6 import uuid7

from src.pipeline.steps import run_aggregate, run_post_aggregate_validation


SUCCESS_TARGET_DATE = date(2026, 5, 10)
WARNING_ONLY_TARGET_DATE = date(2026, 5, 11)
PENDING_BATCH_TARGET_DATE = date(2026, 5, 12)
FAILED_REVIEW_TARGET_DATE = date(2026, 5, 13)
MISSING_MAPPING_TARGET_DATE = date(2026, 5, 14)
ORPHAN_TARGET_DATE = date(2026, 5, 15)
MART_MISSING_TARGET_DATE = date(2026, 5, 16)
COUNT_MISMATCH_TARGET_DATE = date(2026, 5, 17)


@pytest.mark.requires_db
def test_post_aggregate_validation_passes_for_valid_target_date_marts(
    test_db_engine,
    test_db_url,
    test_db_schema,
    monkeypatch,
):
    service_id = uuid7()
    app_id = uuid7()
    review_id = uuid7()
    platform_review_id = f"post-aggregate-success-{review_id}"
    monkeypatch.setenv("DATABASE_URL", test_db_url)

    _cleanup_probe(
        test_db_engine,
        target_date=SUCCESS_TARGET_DATE,
        service_id=service_id,
        app_id=app_id,
        review_id=review_id,
        platform_review_id=platform_review_id,
    )
    try:
        with test_db_engine.begin() as connection:
            _insert_complete_upstream_rows(
                connection,
                target_date=SUCCESS_TARGET_DATE,
                service_id=service_id,
                app_id=app_id,
                review_id=review_id,
                platform_review_id=platform_review_id,
                include_metadata=True,
                include_ingestion_batch=True,
            )

        aggregate_result = run_aggregate(target_date=SUCCESS_TARGET_DATE.isoformat())
        assert aggregate_result.status == "success", aggregate_result.message

        result = run_post_aggregate_validation(target_date=SUCCESS_TARGET_DATE.isoformat())

        assert result.status == "success", result.message
        validations = result.validations
        assert validations is not None
        assert validations["warnings"] == []
        _assert_check(validations, "mart_freshness", passed=True)
        _assert_check(validations, "mart_count_consistency", passed=True)
    finally:
        _cleanup_probe(
            test_db_engine,
            target_date=SUCCESS_TARGET_DATE,
            service_id=service_id,
            app_id=app_id,
            review_id=review_id,
            platform_review_id=platform_review_id,
        )


@pytest.mark.requires_db
def test_post_aggregate_validation_treats_zero_fresh_ingestion_as_warning_only(
    test_db_url,
    test_db_schema,
    monkeypatch,
):
    monkeypatch.setenv("DATABASE_URL", test_db_url)

    result = run_post_aggregate_validation(target_date=WARNING_ONLY_TARGET_DATE.isoformat())

    assert result.status == "success", result.message
    validations = result.validations
    assert validations is not None
    assert validations["warnings"] == ["fresh_ingestion"]
    _assert_check(validations, "fresh_ingestion", passed=False)


@pytest.mark.requires_db
def test_post_aggregate_validation_fails_when_target_date_batch_is_pending(
    test_db_engine,
    test_db_url,
    test_db_schema,
    monkeypatch,
):
    storage_path = "s3://reai-data/bronze/post-aggregate/pending.parquet"
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
                        'post.aggregate.pending',
                        'Post Aggregate Pending',
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
                    "created_at": _target_datetime(PENDING_BATCH_TARGET_DATE),
                },
            )

        result = run_post_aggregate_validation(target_date=PENDING_BATCH_TARGET_DATE.isoformat())

        assert result.status == "failed"
        check = _assert_check(result.validations, "batch_state", passed=False)
        assert check["metrics"]["by_status"] == {"PENDING": 1}
    finally:
        with test_db_engine.begin() as connection:
            connection.execute(
                text("DELETE FROM ingestion_batch WHERE storage_path = :storage_path"),
                {"storage_path": storage_path},
            )


@pytest.mark.requires_db
def test_post_aggregate_validation_fails_retry_exhausted_review_state(
    test_db_engine,
    test_db_url,
    test_db_schema,
    monkeypatch,
):
    service_id = uuid7()
    app_id = uuid7()
    review_id = uuid7()
    platform_review_id = f"post-aggregate-failed-{review_id}"
    monkeypatch.setenv("DATABASE_URL", test_db_url)

    _cleanup_probe(
        test_db_engine,
        target_date=FAILED_REVIEW_TARGET_DATE,
        service_id=service_id,
        app_id=app_id,
        review_id=review_id,
        platform_review_id=platform_review_id,
    )
    try:
        with test_db_engine.begin() as connection:
            _insert_app_context(
                connection,
                service_id=service_id,
                app_id=app_id,
                include_metadata=True,
                target_date=FAILED_REVIEW_TARGET_DATE,
            )
            _insert_review_master_index(
                connection,
                target_date=FAILED_REVIEW_TARGET_DATE,
                service_id=service_id,
                app_id=app_id,
                review_id=review_id,
                platform_review_id=platform_review_id,
                processing_status="FAILED",
                retry_count=3,
            )

        result = run_post_aggregate_validation(target_date=FAILED_REVIEW_TARGET_DATE.isoformat())

        assert result.status == "failed"
        check = _assert_check(result.validations, "review_state", passed=False)
        assert check["metrics"]["retry_exhausted_failed_count"] == 1
    finally:
        _cleanup_probe(
            test_db_engine,
            target_date=FAILED_REVIEW_TARGET_DATE,
            service_id=service_id,
            app_id=app_id,
            review_id=review_id,
            platform_review_id=platform_review_id,
        )


@pytest.mark.requires_db
def test_post_aggregate_validation_fails_missing_service_metadata_mapping(
    test_db_engine,
    test_db_url,
    test_db_schema,
    monkeypatch,
):
    service_id = uuid7()
    app_id = uuid7()
    review_id = uuid7()
    platform_review_id = f"post-aggregate-mapping-{review_id}"
    monkeypatch.setenv("DATABASE_URL", test_db_url)

    _cleanup_probe(
        test_db_engine,
        target_date=MISSING_MAPPING_TARGET_DATE,
        service_id=service_id,
        app_id=app_id,
        review_id=review_id,
        platform_review_id=platform_review_id,
    )
    try:
        with test_db_engine.begin() as connection:
            _insert_complete_upstream_rows(
                connection,
                target_date=MISSING_MAPPING_TARGET_DATE,
                service_id=service_id,
                app_id=app_id,
                review_id=review_id,
                platform_review_id=platform_review_id,
                include_metadata=False,
                include_ingestion_batch=True,
            )

        result = run_post_aggregate_validation(target_date=MISSING_MAPPING_TARGET_DATE.isoformat())

        assert result.status == "failed"
        check = _assert_check(result.validations, "metadata_mapping", passed=False)
        assert check["metrics"]["missing_active_mapping"] == 1
    finally:
        _cleanup_probe(
            test_db_engine,
            target_date=MISSING_MAPPING_TARGET_DATE,
            service_id=service_id,
            app_id=app_id,
            review_id=review_id,
            platform_review_id=platform_review_id,
        )


@pytest.mark.requires_db
def test_post_aggregate_validation_fails_analyzed_orphan_rows(
    test_db_engine,
    test_db_url,
    test_db_schema,
    monkeypatch,
):
    service_id = uuid7()
    app_id = uuid7()
    review_id = uuid7()
    platform_review_id = f"post-aggregate-orphan-{review_id}"
    monkeypatch.setenv("DATABASE_URL", test_db_url)

    _cleanup_probe(
        test_db_engine,
        target_date=ORPHAN_TARGET_DATE,
        service_id=service_id,
        app_id=app_id,
        review_id=review_id,
        platform_review_id=platform_review_id,
    )
    try:
        with test_db_engine.begin() as connection:
            _insert_app_context(
                connection,
                service_id=service_id,
                app_id=app_id,
                include_metadata=True,
                target_date=ORPHAN_TARGET_DATE,
            )
            _insert_review_master_index(
                connection,
                target_date=ORPHAN_TARGET_DATE,
                service_id=service_id,
                app_id=app_id,
                review_id=review_id,
                platform_review_id=platform_review_id,
            )

        result = run_post_aggregate_validation(target_date=ORPHAN_TARGET_DATE.isoformat())

        assert result.status == "failed"
        check = _assert_check(result.validations, "orphan_integrity", passed=False)
        assert check["metrics"] == {
            "missing_action_analysis": 1,
            "missing_app_review": 1,
            "missing_preprocessed": 1,
            "missing_aspects": 1,
        }
    finally:
        _cleanup_probe(
            test_db_engine,
            target_date=ORPHAN_TARGET_DATE,
            service_id=service_id,
            app_id=app_id,
            review_id=review_id,
            platform_review_id=platform_review_id,
        )


@pytest.mark.requires_db
def test_post_aggregate_validation_fails_when_marts_are_missing_for_analyzed_upstream(
    test_db_engine,
    test_db_url,
    test_db_schema,
    monkeypatch,
):
    service_id = uuid7()
    app_id = uuid7()
    review_id = uuid7()
    platform_review_id = f"post-aggregate-mart-missing-{review_id}"
    monkeypatch.setenv("DATABASE_URL", test_db_url)

    _cleanup_probe(
        test_db_engine,
        target_date=MART_MISSING_TARGET_DATE,
        service_id=service_id,
        app_id=app_id,
        review_id=review_id,
        platform_review_id=platform_review_id,
    )
    try:
        with test_db_engine.begin() as connection:
            _insert_complete_upstream_rows(
                connection,
                target_date=MART_MISSING_TARGET_DATE,
                service_id=service_id,
                app_id=app_id,
                review_id=review_id,
                platform_review_id=platform_review_id,
                include_metadata=True,
                include_ingestion_batch=True,
            )

        result = run_post_aggregate_validation(target_date=MART_MISSING_TARGET_DATE.isoformat())

        assert result.status == "failed"
        check = _assert_check(result.validations, "mart_freshness", passed=False)
        assert check["metrics"]["analyzed_count"] == 1
        assert set(check["metrics"]["row_counts"]) == {
            "fact_service_review_daily",
            "fact_service_aspect_daily",
            "fact_category_radar_scores",
            "srv_daily_review_list",
        }
    finally:
        _cleanup_probe(
            test_db_engine,
            target_date=MART_MISSING_TARGET_DATE,
            service_id=service_id,
            app_id=app_id,
            review_id=review_id,
            platform_review_id=platform_review_id,
        )


@pytest.mark.requires_db
def test_post_aggregate_validation_fails_mart_count_mismatch(
    test_db_engine,
    test_db_url,
    test_db_schema,
    monkeypatch,
):
    service_id = uuid7()
    app_id = uuid7()
    review_id = uuid7()
    platform_review_id = f"post-aggregate-count-mismatch-{review_id}"
    monkeypatch.setenv("DATABASE_URL", test_db_url)

    _cleanup_probe(
        test_db_engine,
        target_date=COUNT_MISMATCH_TARGET_DATE,
        service_id=service_id,
        app_id=app_id,
        review_id=review_id,
        platform_review_id=platform_review_id,
    )
    try:
        with test_db_engine.begin() as connection:
            _insert_complete_upstream_rows(
                connection,
                target_date=COUNT_MISMATCH_TARGET_DATE,
                service_id=service_id,
                app_id=app_id,
                review_id=review_id,
                platform_review_id=platform_review_id,
                include_metadata=True,
                include_ingestion_batch=True,
            )

        aggregate_result = run_aggregate(target_date=COUNT_MISMATCH_TARGET_DATE.isoformat())
        assert aggregate_result.status == "success", aggregate_result.message
        with test_db_engine.begin() as connection:
            connection.execute(
                text(
                    """
                    UPDATE fact_service_review_daily
                    SET total_review_cnt = 99
                    WHERE date = :target_date
                      AND service_id = :service_id
                    """
                ),
                {"target_date": COUNT_MISMATCH_TARGET_DATE, "service_id": service_id},
            )

        result = run_post_aggregate_validation(target_date=COUNT_MISMATCH_TARGET_DATE.isoformat())

        assert result.status == "failed"
        check = _assert_check(result.validations, "mart_count_consistency", passed=False)
        assert check["metrics"]["expected_analyzed_review_count"] == 1
        assert check["metrics"]["fact_service_review_daily_total"] == 99
    finally:
        _cleanup_probe(
            test_db_engine,
            target_date=COUNT_MISMATCH_TARGET_DATE,
            service_id=service_id,
            app_id=app_id,
            review_id=review_id,
            platform_review_id=platform_review_id,
        )


def _assert_check(validations, check_name: str, *, passed: bool):
    check = next(check for check in validations["checks"] if check["name"] == check_name)
    assert check["passed"] is passed
    return check


def _target_datetime(target_date: date) -> datetime:
    return datetime(target_date.year, target_date.month, target_date.day, 9, 30, tzinfo=timezone.utc)


def _insert_app_context(
    connection,
    *,
    service_id,
    app_id,
    include_metadata: bool,
    target_date: date,
) -> None:
    connection.execute(
        text(
            """
            INSERT INTO app_service (service_id, service_name)
            VALUES (:service_id, 'Post Aggregate Validation Bank')
            """
        ),
        {"service_id": service_id},
    )
    connection.execute(
        text(
            """
            INSERT INTO apps (app_id, platform_app_id, platform_type, name)
            VALUES (:app_id, :platform_app_id, 'APPSTORE', 'Post Aggregate Validation App')
            """
        ),
        {"app_id": app_id, "platform_app_id": f"post.aggregate.{app_id}"},
    )
    if include_metadata:
        connection.execute(
            text(
                """
                INSERT INTO app_metadata (
                    app_id,
                    service_id,
                    app_type,
                    valid_from,
                    valid_to,
                    is_active
                )
                VALUES (
                    :app_id,
                    :service_id,
                    'CONSUMER',
                    :valid_from,
                    NULL,
                    TRUE
                )
                """
            ),
            {
                "app_id": app_id,
                "service_id": service_id,
                "valid_from": target_date,
            },
        )


def _insert_review_master_index(
    connection,
    *,
    target_date: date,
    service_id,
    app_id,
    review_id,
    platform_review_id: str,
    processing_status: str = "ANALYZED",
    retry_count: int = 0,
) -> None:
    reviewed_at = _target_datetime(target_date)
    connection.execute(
        text(
            """
            INSERT INTO review_master_index (
                review_id,
                app_id,
                service_id,
                platform_review_id,
                platform_type,
                review_created_at,
                ingested_at,
                processing_status,
                parquet_written_at,
                storage_path,
                retry_count,
                is_active,
                is_reply
            )
            VALUES (
                :review_id,
                :app_id,
                :service_id,
                :platform_review_id,
                'APPSTORE',
                :reviewed_at,
                :reviewed_at,
                :processing_status,
                :reviewed_at,
                :storage_path,
                :retry_count,
                TRUE,
                FALSE
            )
            """
        ),
        {
            "review_id": review_id,
            "app_id": app_id,
            "service_id": service_id,
            "platform_review_id": platform_review_id,
            "reviewed_at": reviewed_at,
            "processing_status": processing_status,
            "storage_path": f"s3://reai-data/bronze/post-aggregate/{review_id}.parquet",
            "retry_count": retry_count,
        },
    )


def _insert_complete_upstream_rows(
    connection,
    *,
    target_date: date,
    service_id,
    app_id,
    review_id,
    platform_review_id: str,
    include_metadata: bool,
    include_ingestion_batch: bool,
) -> None:
    reviewed_at = _target_datetime(target_date)
    _insert_app_context(
        connection,
        service_id=service_id,
        app_id=app_id,
        include_metadata=include_metadata,
        target_date=target_date,
    )
    if include_ingestion_batch:
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
                    updated_at,
                    loaded_at
                )
                VALUES (
                    'APPSTORE',
                    :platform_app_id,
                    'Post Aggregate Validation App',
                    :storage_path,
                    1,
                    'LOADED',
                    :reviewed_at,
                    :reviewed_at,
                    :reviewed_at
                )
                """
            ),
            {
                "platform_app_id": f"post.aggregate.{app_id}",
                "storage_path": f"s3://reai-data/bronze/post-aggregate/batch-{review_id}.parquet",
                "reviewed_at": reviewed_at,
            },
        )
    _insert_review_master_index(
        connection,
        target_date=target_date,
        service_id=service_id,
        app_id=app_id,
        review_id=review_id,
        platform_review_id=platform_review_id,
    )
    connection.execute(
        text(
            """
            INSERT INTO app_reviews (
                review_id,
                app_id,
                platform_type,
                country_code,
                platform_review_id,
                reviewer_name,
                review_text,
                rating,
                app_version,
                reviewed_at,
                is_reply,
                reply_comment
            )
            VALUES (
                :review_id,
                :app_id,
                'APPSTORE',
                'kr',
                :platform_review_id,
                'post-aggregate-reviewer',
                'login is slow but usable',
                4,
                '1.0.0',
                :reviewed_at,
                FALSE,
                NULL
            )
            """
        ),
        {
            "review_id": review_id,
            "app_id": app_id,
            "platform_review_id": platform_review_id,
            "reviewed_at": reviewed_at,
        },
    )
    connection.execute(
        text(
            """
            INSERT INTO reviews_preprocessed (
                review_id,
                app_review_id,
                platform_review_id,
                refined_text
            )
            VALUES (
                :review_id,
                :review_id,
                :platform_review_id,
                'login is slow but usable'
            )
            """
        ),
        {"review_id": review_id, "platform_review_id": platform_review_id},
    )
    connection.execute(
        text(
            """
            INSERT INTO review_action_analysis (
                review_id,
                is_action_required,
                action_confidence_score,
                trigger_reason,
                is_attention_required,
                is_verified,
                analyzed_at,
                review_summary
            )
            VALUES (
                :review_id,
                TRUE,
                0.91,
                'slow login',
                TRUE,
                TRUE,
                :reviewed_at,
                'Login is slow but still usable'
            )
            """
        ),
        {"review_id": review_id, "reviewed_at": reviewed_at},
    )
    connection.execute(
        text(
            """
            INSERT INTO review_aspects (
                review_id,
                keyword,
                sentiment_score,
                category
            )
            VALUES
                (:review_id, 'login', 0.75, 'USABILITY'),
                (:review_id, 'speed', 0.65, 'SPEED')
            """
        ),
        {"review_id": review_id},
    )
    connection.execute(
        text(
            """
            INSERT INTO reviews_assigned (
                review_id,
                assigned_dept,
                assignment_reason,
                confidence,
                is_failed,
                try_number
            )
            VALUES (
                :review_id,
                ARRAY['CX', 'APP'],
                'post aggregate validation seed',
                0.88,
                FALSE,
                1
            )
            """
        ),
        {"review_id": review_id},
    )


def _cleanup_probe(
    test_db_engine,
    *,
    target_date: date,
    service_id,
    app_id,
    review_id,
    platform_review_id: str,
) -> None:
    partition_name = f"srv_daily_review_list_{target_date.strftime('%Y_%m_%d')}"
    if not re.fullmatch(r"srv_daily_review_list_\d{4}_\d{2}_\d{2}", partition_name):
        raise ValueError(f"Unexpected partition name: {partition_name}")

    mart_params = {"service_id": service_id, "target_date": target_date}
    with test_db_engine.begin() as connection:
        connection.execute(text(f'DROP TABLE IF EXISTS public."{partition_name}"'))
        connection.execute(
            text(
                """
                DELETE FROM fact_service_review_daily
                WHERE service_id = :service_id
                  AND date = :target_date
                """
            ),
            mart_params,
        )
        connection.execute(
            text(
                """
                DELETE FROM fact_service_aspect_daily
                WHERE service_id = :service_id
                  AND date = :target_date
                """
            ),
            mart_params,
        )
        connection.execute(
            text(
                """
                DELETE FROM fact_category_radar_scores
                WHERE service_id = :service_id
                  AND date = :target_date
                """
            ),
            mart_params,
        )
        connection.execute(
            text("DELETE FROM reviews_assigned WHERE review_id = :review_id"),
            {"review_id": review_id},
        )
        connection.execute(
            text("DELETE FROM review_action_analysis WHERE review_id = :review_id"),
            {"review_id": review_id},
        )
        connection.execute(
            text("DELETE FROM review_aspects WHERE review_id = :review_id"),
            {"review_id": review_id},
        )
        connection.execute(
            text("DELETE FROM reviews_preprocessed WHERE review_id = :review_id"),
            {"review_id": review_id},
        )
        connection.execute(
            text(
                """
                DELETE FROM app_reviews
                WHERE review_id = :review_id
                   OR platform_review_id = :platform_review_id
                """
            ),
            {"review_id": review_id, "platform_review_id": platform_review_id},
        )
        connection.execute(
            text(
                """
                DELETE FROM review_master_index
                WHERE review_id = :review_id
                   OR platform_review_id = :platform_review_id
                """
            ),
            {"review_id": review_id, "platform_review_id": platform_review_id},
        )
        connection.execute(
            text("DELETE FROM ingestion_batch WHERE storage_path LIKE :storage_path_pattern"),
            {"storage_path_pattern": f"%{review_id}%"},
        )
        connection.execute(text("DELETE FROM app_metadata WHERE app_id = :app_id"), {"app_id": app_id})
        connection.execute(text("DELETE FROM apps WHERE app_id = :app_id"), {"app_id": app_id})
        connection.execute(
            text("DELETE FROM app_service WHERE service_id = :service_id"),
            {"service_id": service_id},
        )
