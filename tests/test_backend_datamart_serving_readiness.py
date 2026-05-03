"""Production-like serving-readiness tests for backend datamarts.

The tests in this module avoid direct inserts into mart tables. They seed the
upstream pipeline tables, invoke the real pipeline CLI aggregate step, and then
assert backend-facing mart rows and semantics from PostgreSQL.
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from pathlib import Path

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import NullPool
from uuid6 import uuid7

from src.pipeline import cli as pipeline_cli


TARGET_DATE = date(2026, 5, 3)
SERVICE_NAME = "Serving Readiness Bank"
PLATFORM_REVIEW_PREFIX = "serving-readiness"
MANUAL_PROOF_DOC = (
    Path(__file__).resolve().parents[1]
    / "docs"
    / "backend-datamart-serving-readiness.md"
)
EXPECTED_TABLES = (
    "fact_service_review_daily",
    "fact_service_aspect_daily",
    "fact_category_radar_scores",
    "srv_daily_review_list",
)


@pytest.mark.requires_db
def test_backend_datamarts_are_populated_by_real_pipeline_cli_aggregate_step(
    test_db_url,
    test_db_schema,
    monkeypatch,
):
    """Seed analyzed upstream rows, run CLI aggregate, then assert mart output."""
    engine = create_engine(test_db_url, poolclass=NullPool)
    SessionLocal = sessionmaker(bind=engine)
    service_id = uuid7()
    app_id = uuid7()
    review_ids = [uuid7(), uuid7(), uuid7()]
    platform_review_ids = [
        f"{PLATFORM_REVIEW_PREFIX}-{review_id}" for review_id in review_ids
    ]

    try:
        seed_session = SessionLocal()
        try:
            _seed_analyzed_pipeline_rows(
                seed_session,
                service_id=service_id,
                app_id=app_id,
                review_ids=review_ids,
                platform_review_ids=platform_review_ids,
            )
            seed_session.commit()
        finally:
            seed_session.close()

        monkeypatch.setenv("DATABASE_URL", test_db_url)
        assert (
            pipeline_cli.main(
                ["--steps", "aggregate", "--target-date", TARGET_DATE.isoformat()]
            )
            == 0
        )

        assert_session = SessionLocal()
        try:
            row_counts = _fetch_row_counts(assert_session, service_id)
            assert row_counts == {
                "fact_service_review_daily": 1,
                "fact_service_aspect_daily": 3,
                "fact_category_radar_scores": 2,
                "srv_daily_review_list": 3,
            }

            summary = assert_session.execute(
                text(
                    """
                    SELECT total_review_cnt,
                           action_required_cnt,
                           attention_required_cnt,
                           pos_count,
                           neg_count,
                           ROUND(avg_rating::numeric, 4) AS avg_rating,
                           action_ratio
                    FROM fact_service_review_daily
                    WHERE date = :target_date
                      AND service_id = :service_id
                      AND platform_type = 'APPSTORE'
                    """
                ),
                {"target_date": TARGET_DATE, "service_id": service_id},
            ).one()
            assert summary.total_review_cnt == 3
            assert summary.action_required_cnt == 2
            assert summary.attention_required_cnt == 2
            assert summary.pos_count == 2
            assert summary.neg_count == 1
            assert float(summary.avg_rating) == 3.3333
            assert summary.action_ratio == 0.6667

            top_aspect = assert_session.execute(
                text(
                    """
                    SELECT keyword, mention_cnt, ROUND(avg_sentiment_score::numeric, 4) AS score
                    FROM fact_service_aspect_daily
                    WHERE date = :target_date
                      AND service_id = :service_id
                    ORDER BY mention_cnt DESC, keyword ASC
                    LIMIT 1
                    """
                ),
                {"target_date": TARGET_DATE, "service_id": service_id},
            ).one()
            assert top_aspect.keyword == "login"
            assert top_aspect.mention_cnt == 2
            assert float(top_aspect.score) == 0.4

            radar = assert_session.execute(
                text(
                    """
                    SELECT category_type::text AS category_type,
                           ROUND(avg_sentiment_score::numeric, 4) AS score,
                           review_cnt
                    FROM fact_category_radar_scores
                    WHERE date = :target_date
                      AND service_id = :service_id
                    ORDER BY category_type
                    """
                ),
                {"target_date": TARGET_DATE, "service_id": service_id},
            ).all()
            assert [
                (row.category_type, float(row.score), row.review_cnt)
                for row in radar
            ] == [
                ("SPEED", 0.9, 1),
                ("USABILITY", 0.4, 2),
            ]

            action_row = assert_session.execute(
                text(
                    """
                    SELECT review_summary,
                           rating,
                           ROUND(sentiment_score::numeric, 4) AS sentiment_score,
                           is_action_required,
                           is_attention_required,
                           assigned_dept,
                           keyword,
                           confidence
                    FROM srv_daily_review_list
                    WHERE date = :target_date
                      AND service_id = :service_id
                      AND is_action_required IS TRUE
                    ORDER BY reviewed_at
                    LIMIT 1
                    """
                ),
                {"target_date": TARGET_DATE, "service_id": service_id},
            ).one()
            assert action_row.review_summary == "Login failure needs CX follow-up"
            assert action_row.rating == 1
            assert float(action_row.sentiment_score) == 0.2
            assert action_row.is_attention_required is True
            assert action_row.assigned_dept == ["CX", "APP"]
            assert action_row.keyword == ["login"]
            assert action_row.confidence == 0.93
        finally:
            assert_session.close()
    finally:
        cleanup_session = SessionLocal()
        try:
            _cleanup_serving_readiness_rows(
                cleanup_session,
                service_id=service_id,
                app_id=app_id,
                review_ids=review_ids,
                platform_review_ids=platform_review_ids,
            )
            cleanup_session.commit()
        finally:
            cleanup_session.close()
            engine.dispose()


def test_backend_datamart_serving_readiness_manual_smoke_doc_is_actionable():
    """Manual release proof must document live crawl evidence outside PR CI."""
    doc = MANUAL_PROOF_DOC.read_text(encoding="utf-8")

    required_markers = [
        "docker compose up -d",
        "PYTHONPATH=. uv run python scripts/crawl_reviews.py",
        "PYTHONPATH=. uv run python scripts/load_reviews.py",
        "PYTHONPATH=. uv run python scripts/cleanse_reviews.py --date",
        "PYTHONPATH=. uv run python -m src.pipeline.cli --steps aggregate --target-date",
        "MinIO",
        "fact_service_review_daily",
        "fact_service_aspect_daily",
        "fact_category_radar_scores",
        "srv_daily_review_list",
        "docs/evidence/backend-datamart-serving-readiness",
        "timestamp",
        "failures or caveats",
    ]
    for marker in required_markers:
        assert marker in doc


def _seed_analyzed_pipeline_rows(
    session,
    *,
    service_id,
    app_id,
    review_ids,
    platform_review_ids,
) -> None:
    reviewed_at = [
        datetime(2026, 5, 3, 9, 0, tzinfo=timezone.utc),
        datetime(2026, 5, 3, 10, 0, tzinfo=timezone.utc),
        datetime(2026, 5, 3, 11, 0, tzinfo=timezone.utc),
    ]
    session.execute(
        text(
            "INSERT INTO app_service (service_id, service_name) "
            "VALUES (:service_id, :service_name)"
        ),
        {"service_id": service_id, "service_name": SERVICE_NAME},
    )
    session.execute(
        text(
            """
            INSERT INTO apps (app_id, platform_app_id, platform_type, name)
            VALUES (:app_id, :platform_app_id, 'APPSTORE', :name)
            """
        ),
        {
            "app_id": app_id,
            "platform_app_id": "serving.readiness.appstore",
            "name": SERVICE_NAME,
        },
    )

    for idx, review_id in enumerate(review_ids):
        session.execute(
            text(
                """
                INSERT INTO review_master_index (
                    review_id, app_id, service_id, platform_review_id, platform_type,
                    review_created_at, ingested_at, processing_status,
                    parquet_written_at, storage_path, retry_count, is_active, is_reply
                )
                VALUES (
                    :review_id, :app_id, :service_id, :platform_review_id, 'APPSTORE',
                    :reviewed_at, :reviewed_at, 'ANALYZED',
                    :reviewed_at, :storage_path, 0, TRUE, FALSE
                )
                """
            ),
            {
                "review_id": review_id,
                "app_id": app_id,
                "service_id": service_id,
                "platform_review_id": platform_review_ids[idx],
                "reviewed_at": reviewed_at[idx],
                "storage_path": (
                    "s3://reai-data/bronze/app_reviews/"
                    f"{TARGET_DATE.isoformat()}/{review_id}.parquet"
                ),
            },
        )
        session.execute(
            text(
                """
                INSERT INTO app_reviews (
                    review_id, app_id, platform_type, country_code, platform_review_id,
                    reviewer_name, review_text, rating, app_version, reviewed_at,
                    is_reply, reply_comment
                )
                VALUES (
                    :review_id, :app_id, 'APPSTORE', 'kr', :platform_review_id,
                    :reviewer_name, :review_text, :rating, '9.9.9', :reviewed_at,
                    FALSE, NULL
                )
                """
            ),
            {
                "review_id": review_id,
                "app_id": app_id,
                "platform_review_id": platform_review_ids[idx],
                "reviewer_name": f"serving-readiness-{idx}",
                "review_text": [
                    "Login fails after update and blocks transfers",
                    "Transfers are fast and stable",
                    "Login error appears but the new design is clean",
                ][idx],
                "rating": [1, 5, 4][idx],
                "reviewed_at": reviewed_at[idx],
            },
        )
        session.execute(
            text(
                """
                INSERT INTO reviews_preprocessed (review_id, platform_review_id, refined_text)
                VALUES (:review_id, :platform_review_id, :refined_text)
                """
            ),
            {
                "review_id": review_id,
                "platform_review_id": platform_review_ids[idx],
                "refined_text": [
                    "login fails after update",
                    "transfer is fast and stable",
                    "login error but clean design",
                ][idx],
            },
        )
        session.execute(
            text(
                """
                INSERT INTO review_action_analysis (
                    review_id, is_action_required, action_confidence_score,
                    trigger_reason, is_attention_required, is_verified,
                    analyzed_at, review_summary
                )
                VALUES (
                    :review_id, :is_action_required, :action_confidence_score,
                    :trigger_reason, :is_attention_required, TRUE,
                    :reviewed_at, :review_summary
                )
                """
            ),
            {
                "review_id": review_id,
                "is_action_required": [True, False, True][idx],
                "action_confidence_score": [0.95, 0.1, 0.77][idx],
                "trigger_reason": ["login failure", "positive speed", "login error"][
                    idx
                ],
                "is_attention_required": [True, False, True][idx],
                "reviewed_at": reviewed_at[idx],
                "review_summary": [
                    "Login failure needs CX follow-up",
                    "Transfer speed is positive",
                    "Login error with positive design note",
                ][idx],
            },
        )
        session.execute(
            text(
                """
                INSERT INTO reviews_assigned (
                    review_id, assigned_dept, assignment_reason,
                    confidence, is_failed, try_number
                )
                VALUES (
                    :review_id, :assigned_dept, :assignment_reason,
                    :confidence, FALSE, 1
                )
                """
            ),
            {
                "review_id": review_id,
                "assigned_dept": [["CX", "APP"], ["APP"], ["CX", "DESIGN"]][idx],
                "assignment_reason": ["login", "speed", "login-design"][idx],
                "confidence": [0.93, 0.88, 0.81][idx],
            },
        )

    aspect_rows = [
        (review_ids[0], "login", 0.2, "USABILITY"),
        (review_ids[1], "transfer", 0.9, "SPEED"),
        (review_ids[2], "login", 0.6, "USABILITY"),
        (review_ids[2], "design", 0.8, "OTHER"),
    ]
    for review_id, keyword, score, category in aspect_rows:
        session.execute(
            text(
                """
                INSERT INTO review_aspects (review_id, keyword, sentiment_score, category)
                VALUES (:review_id, :keyword, :sentiment_score, :category)
                """
            ),
            {
                "review_id": review_id,
                "keyword": keyword,
                "sentiment_score": score,
                "category": category,
            },
        )


def _fetch_row_counts(session, service_id) -> dict[str, int]:
    counts = {}
    for table_name in EXPECTED_TABLES:
        counts[table_name] = session.execute(
            text(
                f"""
                SELECT COUNT(*)
                FROM {table_name}
                WHERE date = :target_date
                  AND service_id = :service_id
                """
            ),
            {"target_date": TARGET_DATE, "service_id": service_id},
        ).scalar_one()
    return counts


def _cleanup_serving_readiness_rows(
    session,
    *,
    service_id,
    app_id,
    review_ids,
    platform_review_ids,
) -> None:
    params = {
        "target_date": TARGET_DATE,
        "service_id": service_id,
        "app_id": app_id,
        "review_ids": tuple(review_ids),
        "platform_review_ids": tuple(platform_review_ids),
    }
    session.execute(
        text(
            "DELETE FROM srv_daily_review_list "
            "WHERE date = :target_date AND service_id = :service_id"
        ),
        params,
    )
    session.execute(
        text(
            "DELETE FROM fact_category_radar_scores "
            "WHERE date = :target_date AND service_id = :service_id"
        ),
        params,
    )
    session.execute(
        text(
            "DELETE FROM fact_service_aspect_daily "
            "WHERE date = :target_date AND service_id = :service_id"
        ),
        params,
    )
    session.execute(
        text(
            "DELETE FROM fact_service_review_daily "
            "WHERE date = :target_date AND service_id = :service_id"
        ),
        params,
    )
    session.execute(text("DELETE FROM reviews_assigned WHERE review_id IN :review_ids"), params)
    session.execute(
        text("DELETE FROM review_action_analysis WHERE review_id IN :review_ids"),
        params,
    )
    session.execute(text("DELETE FROM review_aspects WHERE review_id IN :review_ids"), params)
    session.execute(
        text("DELETE FROM reviews_preprocessed WHERE review_id IN :review_ids"),
        params,
    )
    session.execute(
        text("DELETE FROM app_reviews WHERE platform_review_id IN :platform_review_ids"),
        params,
    )
    session.execute(
        text("DELETE FROM review_master_index WHERE review_id IN :review_ids"),
        params,
    )
    session.execute(text("DELETE FROM apps WHERE app_id = :app_id"), params)
    session.execute(text("DELETE FROM app_service WHERE service_id = :service_id"), params)
