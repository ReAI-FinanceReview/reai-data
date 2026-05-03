"""Backend-facing Data Mart contract tests.

These tests intentionally use the real PostgreSQL test fixture. They lock the
schema shape consumed by backend dashboard endpoints without introducing mocks
or a SQLite fallback.
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from pathlib import Path
from uuid import UUID

import pytest
from sqlalchemy import text
from uuid6 import uuid7


CONTRACT_DOC_PATH = (
    Path(__file__).resolve().parents[1] / "docs" / "backend-datamart-contract.md"
)
CONTRACT_TABLES = {
    "fact_service_review_daily": {
        "columns": {
            "date": {"data_type": "date", "nullable": False},
            "service_id": {"data_type": "uuid", "nullable": False},
            "platform_type": {"data_type": "USER-DEFINED", "nullable": False},
            "total_review_cnt": {"data_type": "integer", "nullable": True},
            "action_required_cnt": {"data_type": "integer", "nullable": True},
            "attention_required_cnt": {"data_type": "integer", "nullable": True},
            "pos_count": {"data_type": "integer", "nullable": True},
            "neg_count": {"data_type": "integer", "nullable": True},
            "avg_rating": {"data_type": "double precision", "nullable": True},
            "action_ratio": {"data_type": "double precision", "nullable": True},
        },
        "primary_key": ["date", "service_id", "platform_type"],
        "indexes": {"idx_fact_service_review_daily_date"},
    },
    "fact_service_aspect_daily": {
        "columns": {
            "date": {"data_type": "date", "nullable": False},
            "service_id": {"data_type": "uuid", "nullable": False},
            "keyword": {"data_type": "text", "nullable": False},
            "mention_cnt": {"data_type": "integer", "nullable": True},
            "avg_sentiment_score": {
                "data_type": "double precision",
                "nullable": True,
            },
        },
        "primary_key": ["date", "service_id", "keyword"],
        "indexes": {"idx_fact_service_aspect_daily_date"},
    },
    "fact_category_radar_scores": {
        "columns": {
            "date": {"data_type": "date", "nullable": False},
            "service_id": {"data_type": "uuid", "nullable": False},
            "category_type": {"data_type": "USER-DEFINED", "nullable": False},
            "avg_sentiment_score": {
                "data_type": "double precision",
                "nullable": True,
            },
            "review_cnt": {"data_type": "integer", "nullable": True},
        },
        "primary_key": ["date", "service_id", "category_type"],
        "indexes": {"idx_fact_category_radar_scores_date"},
    },
    "srv_daily_review_list": {
        "columns": {
            "review_id": {"data_type": "uuid", "nullable": False},
            "date": {"data_type": "date", "nullable": False},
            "service_id": {"data_type": "uuid", "nullable": True},
            "refined_text": {"data_type": "text", "nullable": True},
            "review_summary": {"data_type": "text", "nullable": True},
            "rating": {"data_type": "integer", "nullable": True},
            "reviewed_at": {
                "data_type": "timestamp with time zone",
                "nullable": True,
            },
            "sentiment_score": {"data_type": "double precision", "nullable": True},
            "is_action_required": {"data_type": "boolean", "nullable": True},
            "is_attention_required": {"data_type": "boolean", "nullable": True},
            "assigned_dept": {"data_type": "ARRAY", "nullable": True},
            "keyword": {"data_type": "ARRAY", "nullable": True},
            "confidence": {"data_type": "double precision", "nullable": True},
        },
        "primary_key": ["review_id", "date"],
        "indexes": {
            "idx_srv_daily_review_list_service_id",
            "idx_srv_daily_review_list_date",
            "idx_srv_daily_review_list_is_action_required",
            "idx_srv_daily_review_list_keyword",
        },
    },
}

SERVICE_FK_TABLES = set(CONTRACT_TABLES)
CONTRACT_TARGET_DATE = date(2026, 5, 2)
CONTRACT_PARTITION_NAME = "srv_daily_review_list_contract_2026_05_02"
READINESS_TARGET_DATE = date(2026, 5, 3)
READINESS_PARTITION_NAME = "srv_daily_review_list_2026_05_03"
CONTRACT_DOC_MARKERS = {
    "## Contract boundary": "contract table list",
    "## Column contract matrices": "column contract section",
    "Date/timezone semantics": "date/timezone semantics",
    "Nullable `srv_daily_review_list.service_id`": "nullable service_id handling",
    "TTL/partition retention": "TTL/partition retention",
    "Breaking-change policy": "breaking-change policy",
    "Docker validation and verification commands": "Docker validation",
    "Backend query examples": "backend query examples",
    "Service summary dashboard": "summary dashboard query",
    "Keyword/aspect trend": "aspect trend query",
    "Radar chart": "radar chart query",
    "Review list with stable pagination order": "review list query",
    "Current serving indexes support service, date, action-required, and keyword filtering": (
        "pagination index limitation"
    ),
    "## Serving readiness proof": "production-like serving readiness proof",
    "## Manual live crawl smoke proof": "manual live crawl smoke proof",
}


@pytest.mark.requires_db
def test_backend_datamart_columns_match_contract(test_db_session):
    """Backend DTO fields must exist with stable PostgreSQL types/nullability."""
    result = test_db_session.execute(
        text(
            """
            SELECT table_name, column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name IN :table_names
            """
        ),
        {"table_names": tuple(CONTRACT_TABLES)},
    )

    actual_columns = {
        (row.table_name, row.column_name): {
            "data_type": row.data_type,
            "nullable": row.is_nullable == "YES",
        }
        for row in result
    }

    for table_name, contract in CONTRACT_TABLES.items():
        for column_name, expected in contract["columns"].items():
            actual = actual_columns.get((table_name, column_name))
            assert actual is not None, f"{table_name}.{column_name} is missing"
            assert actual["data_type"] == expected["data_type"], (
                f"{table_name}.{column_name} type mismatch: "
                f"expected {expected['data_type']}, got {actual['data_type']}"
            )
            assert actual["nullable"] is expected["nullable"], (
                f"{table_name}.{column_name} nullability mismatch: "
                f"expected nullable={expected['nullable']}, "
                f"got nullable={actual['nullable']}"
            )


def test_backend_datamart_contract_doc_is_complete():
    """The backend handoff doc must cover required contract and query topics."""
    contract_doc = CONTRACT_DOC_PATH.read_text(encoding="utf-8")

    for table_name in CONTRACT_TABLES:
        assert table_name in contract_doc

    for marker, description in CONTRACT_DOC_MARKERS.items():
        assert marker in contract_doc, f"Missing {description}: {marker}"

    assert "Removes, renames, or repurposes a contract table" in contract_doc
    assert "Removes, renames, or repurposes a contract column" in contract_doc
    assert "Changes PostgreSQL type, enum domain, nullability, primary key" in contract_doc
    assert "Alembic revision" in contract_doc
    assert "migration impact" in contract_doc
    assert "service_id = :service_id" in contract_doc
    assert "exclude rows where `service_id IS NULL`" in contract_doc
    assert "Backend API endpoints" in contract_doc
    assert "New `fact_*`, `dim_*`, `srv_*`, view, or materialized-view tables" in contract_doc


@pytest.mark.requires_db
def test_backend_datamart_primary_keys_match_contract(test_db_session):
    """Upserts and cursor-style backend reads depend on deterministic keys."""
    for table_name, contract in CONTRACT_TABLES.items():
        result = test_db_session.execute(
            text(
                """
                SELECT attribute.attname
                FROM pg_index index
                JOIN pg_class table_class
                  ON table_class.oid = index.indrelid
                JOIN pg_namespace namespace
                  ON namespace.oid = table_class.relnamespace
                CROSS JOIN LATERAL unnest(index.indkey)
                  WITH ORDINALITY AS key_columns(attnum, ordinal)
                JOIN pg_attribute attribute
                  ON attribute.attrelid = table_class.oid
                 AND attribute.attnum = key_columns.attnum
                WHERE namespace.nspname = 'public'
                  AND table_class.relname = :table_name
                  AND index.indisprimary
                ORDER BY key_columns.ordinal
                """
            ),
            {"table_name": table_name},
        )

        actual_key = [row.attname for row in result]
        assert actual_key == contract["primary_key"], (
            f"{table_name} primary key mismatch: "
            f"expected {contract['primary_key']}, got {actual_key}"
        )


@pytest.mark.requires_db
def test_backend_datamart_indexes_and_foreign_keys_are_query_ready(test_db_session):
    """Dashboard filters need service/date/action indexes and app_service FKs."""
    index_rows = test_db_session.execute(
        text(
            """
            SELECT tablename, indexname
            FROM pg_indexes
            WHERE schemaname = 'public'
              AND tablename IN :table_names
            """
        ),
        {"table_names": tuple(CONTRACT_TABLES)},
    )
    indexes_by_table: dict[str, set[str]] = {}
    for row in index_rows:
        indexes_by_table.setdefault(row.tablename, set()).add(row.indexname)

    for table_name, contract in CONTRACT_TABLES.items():
        missing_indexes = contract["indexes"] - indexes_by_table.get(table_name, set())
        assert not missing_indexes, f"{table_name} missing indexes: {missing_indexes}"

    fk_rows = test_db_session.execute(
        text(
            """
            SELECT
                source.relname AS table_name,
                target.relname AS referred_table,
                delete_rule.confdeltype AS delete_rule
            FROM pg_constraint delete_rule
            JOIN pg_class source
              ON source.oid = delete_rule.conrelid
            JOIN pg_class target
              ON target.oid = delete_rule.confrelid
            JOIN pg_namespace namespace
              ON namespace.oid = source.relnamespace
            WHERE namespace.nspname = 'public'
              AND delete_rule.contype = 'f'
              AND source.relname IN :table_names
            """
        ),
        {"table_names": tuple(SERVICE_FK_TABLES)},
    )
    service_fk_tables = {
        row.table_name
        for row in fk_rows
        if row.referred_table == "app_service" and row.delete_rule == "c"
    }
    assert service_fk_tables == SERVICE_FK_TABLES


@pytest.mark.requires_db
def test_backend_datamart_review_list_is_range_partitioned(test_db_session):
    """The hot review-list table must remain range-partitioned by date."""
    row = test_db_session.execute(
        text(
            """
            SELECT table_class.relkind, partitioned.partstrat
            FROM pg_class table_class
            JOIN pg_namespace namespace
              ON namespace.oid = table_class.relnamespace
            JOIN pg_partitioned_table partitioned
              ON partitioned.partrelid = table_class.oid
            WHERE namespace.nspname = 'public'
              AND table_class.relname = 'srv_daily_review_list'
            """
        )
    ).one()

    assert row.relkind == "p"
    assert row.partstrat == "r"


@pytest.mark.requires_db
def test_backend_datamart_accepts_backend_dashboard_read_shapes(test_db_session):
    """Insert contract rows and prove backend-style queries work on PostgreSQL."""
    service_id = uuid7()
    review_id = uuid7()
    _create_contract_partition(test_db_session)
    _insert_contract_rows(test_db_session, service_id, review_id)

    summary_row = test_db_session.execute(
        text(
            """
            SELECT
                total_review_cnt,
                action_required_cnt,
                attention_required_cnt,
                avg_rating,
                action_ratio
            FROM fact_service_review_daily
            WHERE service_id = :service_id
              AND date = :target_date
              AND platform_type = 'APPSTORE'
            """
        ),
        {"service_id": service_id, "target_date": CONTRACT_TARGET_DATE},
    ).one()
    assert summary_row.total_review_cnt == 3
    assert summary_row.action_required_cnt == 1
    assert summary_row.attention_required_cnt == 2
    assert summary_row.avg_rating == 4.25
    assert summary_row.action_ratio == 0.3333

    aspect_row = test_db_session.execute(
        text(
            """
            SELECT keyword, mention_cnt, avg_sentiment_score
            FROM fact_service_aspect_daily
            WHERE service_id = :service_id
              AND date = :target_date
            ORDER BY mention_cnt DESC
            LIMIT 1
            """
        ),
        {"service_id": service_id, "target_date": CONTRACT_TARGET_DATE},
    ).one()
    assert aspect_row.keyword == "login"
    assert aspect_row.mention_cnt == 2
    assert aspect_row.avg_sentiment_score == 0.75

    radar_row = test_db_session.execute(
        text(
            """
            SELECT category_type::text, avg_sentiment_score, review_cnt
            FROM fact_category_radar_scores
            WHERE service_id = :service_id
              AND date = :target_date
              AND category_type = 'USABILITY'
            """
        ),
        {"service_id": service_id, "target_date": CONTRACT_TARGET_DATE},
    ).one()
    assert radar_row.category_type == "USABILITY"
    assert radar_row.avg_sentiment_score == 0.82
    assert radar_row.review_cnt == 3

    review_row = test_db_session.execute(
        text(
            """
            SELECT
                review_id,
                review_summary,
                rating,
                sentiment_score,
                assigned_dept,
                keyword,
                confidence
            FROM srv_daily_review_list
            WHERE service_id = :service_id
              AND date = :target_date
              AND is_action_required IS TRUE
            """
        ),
        {"service_id": service_id, "target_date": CONTRACT_TARGET_DATE},
    ).one()
    assert UUID(str(review_row.review_id)) == review_id
    assert review_row.review_summary == "Login flow needs attention"
    assert review_row.rating == 2
    assert review_row.sentiment_score == 0.25
    assert review_row.assigned_dept == ["CX", "APP"]
    assert review_row.keyword == ["login", "error"]
    assert review_row.confidence == 0.91


@pytest.mark.requires_db
def test_backend_datamart_readiness_step_generates_semantic_rows(
    test_db_engine,
    test_db_url,
    monkeypatch,
):
    """The real aggregate step must populate all backend-facing mart tables."""
    from src.pipeline.steps import run_steps

    service_id = uuid7()
    app_id = uuid7()
    review_id = uuid7()
    platform_review_id = f"readiness-{review_id}"

    monkeypatch.setenv("DATABASE_URL", test_db_url)

    _cleanup_readiness_probe(
        test_db_engine,
        service_id=service_id,
        app_id=app_id,
        review_id=review_id,
        platform_review_id=platform_review_id,
    )
    try:
        with test_db_engine.begin() as connection:
            _insert_readiness_upstream_rows(
                connection,
                service_id=service_id,
                app_id=app_id,
                review_id=review_id,
                platform_review_id=platform_review_id,
            )

        results = run_steps(["aggregate"], target_date=str(READINESS_TARGET_DATE))

        assert len(results) == 1
        aggregate_result = results[0]
        assert aggregate_result.step == "aggregate"
        assert aggregate_result.status == "success", aggregate_result.message
        assert aggregate_result.as_dict()["validations"] is None

        with test_db_engine.connect() as connection:
            summary_row = connection.execute(
                text(
                    """
                    SELECT
                        total_review_cnt,
                        action_required_cnt,
                        attention_required_cnt,
                        pos_count,
                        neg_count,
                        avg_rating,
                        action_ratio
                    FROM fact_service_review_daily
                    WHERE service_id = :service_id
                      AND date = :target_date
                      AND platform_type = 'APPSTORE'
                    """
                ),
                {"service_id": service_id, "target_date": READINESS_TARGET_DATE},
            ).one()
            assert summary_row.total_review_cnt == 1
            assert summary_row.action_required_cnt == 1
            assert summary_row.attention_required_cnt == 1
            assert summary_row.pos_count == 1
            assert summary_row.neg_count == 0
            assert summary_row.avg_rating == 2
            assert summary_row.action_ratio == 1.0

            aspect_rows = connection.execute(
                text(
                    """
                    SELECT keyword, mention_cnt, avg_sentiment_score
                    FROM fact_service_aspect_daily
                    WHERE service_id = :service_id
                      AND date = :target_date
                    ORDER BY keyword ASC
                    """
                ),
                {"service_id": service_id, "target_date": READINESS_TARGET_DATE},
            ).mappings().all()
            aspect_values = [dict(row) for row in aspect_rows]
            assert aspect_values == [
                {
                    "keyword": "login",
                    "mention_cnt": 1,
                    "avg_sentiment_score": 0.25,
                },
                {
                    "keyword": "speed",
                    "mention_cnt": 1,
                    "avg_sentiment_score": 0.75,
                },
            ]

            radar_rows = connection.execute(
                text(
                    """
                    SELECT category_type::text, avg_sentiment_score, review_cnt
                    FROM fact_category_radar_scores
                    WHERE service_id = :service_id
                      AND date = :target_date
                    ORDER BY category_type ASC
                    """
                ),
                {"service_id": service_id, "target_date": READINESS_TARGET_DATE},
            ).all()
            assert [(row.category_type, row.avg_sentiment_score, row.review_cnt) for row in radar_rows] == [
                ("SPEED", 0.75, 1),
                ("USABILITY", 0.25, 1),
            ]

            review_row = connection.execute(
                text(
                    """
                    SELECT
                        review_id,
                        refined_text,
                        review_summary,
                        rating,
                        sentiment_score,
                        is_action_required,
                        is_attention_required,
                        assigned_dept,
                        keyword,
                        confidence
                    FROM srv_daily_review_list
                    WHERE service_id = :service_id
                      AND date = :target_date
                    """
                ),
                {"service_id": service_id, "target_date": READINESS_TARGET_DATE},
            ).one()
            assert UUID(str(review_row.review_id)) == review_id
            assert review_row.refined_text == "login is broken and slow"
            assert review_row.review_summary == "Login flow is broken and slow"
            assert review_row.rating == 2
            assert review_row.sentiment_score == 0.5
            assert review_row.is_action_required is True
            assert review_row.is_attention_required is True
            assert review_row.assigned_dept == ["CX", "APP"]
            assert set(review_row.keyword) == {"login", "speed"}
            assert review_row.confidence == 0.88
    finally:
        _cleanup_readiness_probe(
            test_db_engine,
            service_id=service_id,
            app_id=app_id,
            review_id=review_id,
            platform_review_id=platform_review_id,
        )


def _create_contract_partition(test_db_session) -> None:
    test_db_session.execute(
        text(
            f"""
            CREATE TABLE IF NOT EXISTS public.{CONTRACT_PARTITION_NAME}
            PARTITION OF public.srv_daily_review_list
            FOR VALUES FROM ('2026-05-02') TO ('2026-05-03')
            """
        )
    )


def _insert_contract_rows(test_db_session, service_id, review_id) -> None:
    reviewed_at = datetime(2026, 5, 2, 9, 30, tzinfo=timezone.utc)
    test_db_session.execute(
        text(
            """
            INSERT INTO app_service (service_id, service_name)
            VALUES (:service_id, 'Contract Banking')
            """
        ),
        {"service_id": service_id},
    )
    test_db_session.execute(
        text(
            """
            INSERT INTO fact_service_review_daily (
                date,
                service_id,
                platform_type,
                total_review_cnt,
                action_required_cnt,
                attention_required_cnt,
                avg_rating,
                pos_count,
                neg_count,
                action_ratio
            )
            VALUES (
                :target_date,
                :service_id,
                'APPSTORE',
                3,
                1,
                2,
                4.25,
                2,
                1,
                0.3333
            )
            """
        ),
        {"service_id": service_id, "target_date": CONTRACT_TARGET_DATE},
    )
    test_db_session.execute(
        text(
            """
            INSERT INTO fact_service_aspect_daily (
                date,
                service_id,
                keyword,
                mention_cnt,
                avg_sentiment_score
            )
            VALUES (:target_date, :service_id, 'login', 2, 0.75)
            """
        ),
        {"service_id": service_id, "target_date": CONTRACT_TARGET_DATE},
    )
    test_db_session.execute(
        text(
            """
            INSERT INTO fact_category_radar_scores (
                date,
                service_id,
                category_type,
                avg_sentiment_score,
                review_cnt
            )
            VALUES (:target_date, :service_id, 'USABILITY', 0.82, 3)
            """
        ),
        {"service_id": service_id, "target_date": CONTRACT_TARGET_DATE},
    )
    test_db_session.execute(
        text(
            """
            INSERT INTO srv_daily_review_list (
                review_id,
                date,
                service_id,
                refined_text,
                review_summary,
                rating,
                reviewed_at,
                sentiment_score,
                is_action_required,
                is_attention_required,
                assigned_dept,
                keyword,
                confidence
            )
            VALUES (
                :review_id,
                :target_date,
                :service_id,
                'login error after update',
                'Login flow needs attention',
                2,
                :reviewed_at,
                0.25,
                TRUE,
                TRUE,
                ARRAY['CX', 'APP'],
                ARRAY['login', 'error'],
                0.91
            )
            """
        ),
        {
            "review_id": review_id,
            "service_id": service_id,
            "target_date": CONTRACT_TARGET_DATE,
            "reviewed_at": reviewed_at,
        },
    )


def _insert_readiness_upstream_rows(
    connection,
    *,
    service_id,
    app_id,
    review_id,
    platform_review_id: str,
) -> None:
    reviewed_at = datetime(2026, 5, 3, 9, 30, tzinfo=timezone.utc)
    connection.execute(
        text(
            """
            INSERT INTO app_service (service_id, service_name)
            VALUES (:service_id, 'Readiness Banking')
            """
        ),
        {"service_id": service_id},
    )
    connection.execute(
        text(
            """
            INSERT INTO apps (app_id, platform_app_id, platform_type, name)
            VALUES (:app_id, 'readiness.appstore', 'APPSTORE', 'Readiness App')
            """
        ),
        {"app_id": app_id},
    )
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
                'ANALYZED',
                :reviewed_at,
                's3://reai-data/bronze/readiness/review.parquet',
                0,
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
        },
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
                'readiness-reviewer',
                'login is broken and slow',
                2,
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
                'login is broken and slow'
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
                0.93,
                'login failure',
                TRUE,
                TRUE,
                :reviewed_at,
                'Login flow is broken and slow'
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
                (:review_id, 'login', 0.25, 'USABILITY'),
                (:review_id, 'speed', 0.75, 'SPEED')
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
                'readiness semantic route',
                0.88,
                FALSE,
                1
            )
            """
        ),
        {"review_id": review_id},
    )


def _cleanup_readiness_probe(
    test_db_engine,
    *,
    service_id,
    app_id,
    review_id,
    platform_review_id: str,
) -> None:
    with test_db_engine.begin() as connection:
        connection.execute(text(f"DROP TABLE IF EXISTS public.{READINESS_PARTITION_NAME}"))
        connection.execute(
            text(
                """
                DELETE FROM fact_service_review_daily
                WHERE service_id = :service_id
                  AND date = :target_date
                """
            ),
            {"service_id": service_id, "target_date": READINESS_TARGET_DATE},
        )
        connection.execute(
            text(
                """
                DELETE FROM fact_service_aspect_daily
                WHERE service_id = :service_id
                  AND date = :target_date
                """
            ),
            {"service_id": service_id, "target_date": READINESS_TARGET_DATE},
        )
        connection.execute(
            text(
                """
                DELETE FROM fact_category_radar_scores
                WHERE service_id = :service_id
                  AND date = :target_date
                """
            ),
            {"service_id": service_id, "target_date": READINESS_TARGET_DATE},
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
        connection.execute(text("DELETE FROM apps WHERE app_id = :app_id"), {"app_id": app_id})
        connection.execute(
            text("DELETE FROM app_service WHERE service_id = :service_id"),
            {"service_id": service_id},
        )
