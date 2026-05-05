"""Post-aggregate database readiness validation for Airflow DAGs."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import date
from typing import Any, Literal

from sqlalchemy import bindparam, text

from src.utils.db_connector import DatabaseConnector


Severity = Literal["failure", "warning", "report"]

RETRY_EXHAUSTED_THRESHOLD = 3
MART_TABLES = (
    "fact_service_review_daily",
    "fact_service_aspect_daily",
    "fact_category_radar_scores",
    "srv_daily_review_list",
)
STUCK_BATCH_STATUSES = ("PENDING", "FAILED", "RETRYING", "DEAD_LETTER")
STUCK_REVIEW_STATUSES = ("RAW", "CLEANED")


@dataclass(frozen=True)
class ValidationCheck:
    """Single post-aggregate validation check result."""

    name: str
    severity: Severity
    passed: bool
    metrics: dict[str, Any] = field(default_factory=dict)
    message: str | None = None

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ValidationReport:
    """Serializable validation report consumed by CLI and Airflow tasks."""

    target_date: str
    status: Literal["success", "failed"]
    warnings: list[str]
    checks: list[ValidationCheck]

    def as_dict(self) -> dict[str, Any]:
        return {
            "target_date": self.target_date,
            "status": self.status,
            "warnings": self.warnings,
            "checks": [check.as_dict() for check in self.checks],
        }


class PostAggregateValidator:
    """Validate target-date DB state after Gold aggregation."""

    def __init__(self, config_path: str | None = None):
        self.db_connector = DatabaseConnector(config_path) if config_path is not None else DatabaseConnector()

    def validate(self, target_date: date) -> ValidationReport:
        session = self.db_connector.get_session()
        try:
            checks = [
                self._check_fresh_ingestion(session, target_date),
                self._check_batch_state(session, target_date),
                self._check_review_state(session, target_date),
                self._check_metadata_mapping(session, target_date),
                self._check_orphan_integrity(session, target_date),
                self._check_mart_freshness(session, target_date),
                self._check_mart_count_consistency(session, target_date),
                self._check_mart_quality(session, target_date),
            ]
        finally:
            session.close()

        failed = [check for check in checks if check.severity == "failure" and not check.passed]
        warnings = [check.name for check in checks if check.severity == "warning" and not check.passed]
        return ValidationReport(
            target_date=target_date.isoformat(),
            status="failed" if failed else "success",
            warnings=warnings,
            checks=checks,
        )

    def _check_fresh_ingestion(self, session, target_date: date) -> ValidationCheck:
        row = session.execute(
            text(
                """
                SELECT COUNT(*) AS batch_count,
                       COALESCE(SUM(record_count), 0) AS record_count
                FROM ingestion_batch
                WHERE DATE_TRUNC('day', created_at)::date = :target_date
                """
            ),
            {"target_date": target_date},
        ).one()
        metrics = {
            "batch_count": int(row.batch_count or 0),
            "record_count": int(row.record_count or 0),
        }
        return ValidationCheck(
            name="fresh_ingestion",
            severity="warning",
            passed=metrics["record_count"] > 0,
            metrics=metrics,
            message="No fresh ingestion rows for target date." if metrics["record_count"] == 0 else None,
        )

    def _check_batch_state(self, session, target_date: date) -> ValidationCheck:
        rows = session.execute(
            text(
                """
                SELECT status::text AS status, COUNT(*) AS count
                FROM ingestion_batch
                WHERE DATE_TRUNC('day', created_at)::date = :target_date
                  AND status::text IN :stuck_statuses
                GROUP BY status
                ORDER BY status
                """
            ).bindparams(bindparam("stuck_statuses", expanding=True)),
            {"target_date": target_date, "stuck_statuses": STUCK_BATCH_STATUSES},
        ).all()
        by_status = {row.status: int(row.count) for row in rows}
        stuck_count = sum(by_status.values())
        return ValidationCheck(
            name="batch_state",
            severity="failure",
            passed=stuck_count == 0,
            metrics={"stuck_count": stuck_count, "by_status": by_status},
            message="Unresolved ingestion batches remain." if stuck_count else None,
        )

    def _check_review_state(self, session, target_date: date) -> ValidationCheck:
        stuck_rows = session.execute(
            text(
                """
                SELECT processing_status::text AS status, COUNT(*) AS count
                FROM review_master_index
                WHERE DATE_TRUNC('day', review_created_at)::date = :target_date
                  AND processing_status::text IN :stuck_statuses
                GROUP BY processing_status
                ORDER BY processing_status
                """
            ).bindparams(bindparam("stuck_statuses", expanding=True)),
            {"target_date": target_date, "stuck_statuses": STUCK_REVIEW_STATUSES},
        ).all()
        failed_exhausted = session.execute(
            text(
                """
                SELECT COUNT(*) AS count
                FROM review_master_index
                WHERE DATE_TRUNC('day', review_created_at)::date = :target_date
                  AND processing_status::text = 'FAILED'
                  AND COALESCE(retry_count, 0) >= :retry_exhausted_threshold
                """
            ),
            {
                "target_date": target_date,
                "retry_exhausted_threshold": RETRY_EXHAUSTED_THRESHOLD,
            },
        ).scalar_one()
        analyzed_count = self._analyzed_count(session, target_date)
        by_status = {row.status: int(row.count) for row in stuck_rows}
        stuck_count = sum(by_status.values()) + int(failed_exhausted or 0)
        return ValidationCheck(
            name="review_state",
            severity="failure",
            passed=stuck_count == 0,
            metrics={
                "analyzed_count": analyzed_count,
                "stuck_count": stuck_count,
                "by_status": by_status,
                "retry_exhausted_failed_count": int(failed_exhausted or 0),
            },
            message="Review state flow has unresolved or retry-exhausted rows." if stuck_count else None,
        )

    def _check_metadata_mapping(self, session, target_date: date) -> ValidationCheck:
        row = session.execute(
            text(
                """
                SELECT
                    COUNT(*) FILTER (WHERE rmi.service_id IS NULL) AS missing_service_id,
                    COUNT(*) FILTER (WHERE rmi.service_id IS NOT NULL AND am.id IS NULL) AS missing_active_mapping
                FROM review_master_index rmi
                LEFT JOIN app_metadata am
                  ON am.app_id = rmi.app_id
                 AND am.service_id = rmi.service_id
                 AND am.is_active IS TRUE
                 AND (am.valid_from IS NULL OR am.valid_from <= :target_date)
                 AND (am.valid_to IS NULL OR am.valid_to >= :target_date)
                WHERE DATE_TRUNC('day', rmi.review_created_at)::date = :target_date
                  AND rmi.processing_status::text = 'ANALYZED'
                """
            ),
            {"target_date": target_date},
        ).one()
        missing_service_id = int(row.missing_service_id or 0)
        missing_active_mapping = int(row.missing_active_mapping or 0)
        missing_count = missing_service_id + missing_active_mapping
        return ValidationCheck(
            name="metadata_mapping",
            severity="failure",
            passed=missing_count == 0,
            metrics={
                "missing_service_id": missing_service_id,
                "missing_active_mapping": missing_active_mapping,
            },
            message="Analyzed reviews have missing service metadata mapping." if missing_count else None,
        )

    def _check_orphan_integrity(self, session, target_date: date) -> ValidationCheck:
        row = session.execute(
            text(
                """
                WITH target AS (
                    SELECT rmi.review_id, rmi.platform_review_id
                    FROM review_master_index rmi
                    WHERE DATE_TRUNC('day', rmi.review_created_at)::date = :target_date
                      AND rmi.processing_status::text = 'ANALYZED'
                )
                SELECT
                    COUNT(*) FILTER (WHERE raa.review_id IS NULL) AS missing_action_analysis,
                    COUNT(*) FILTER (WHERE ar.platform_review_id IS NULL) AS missing_app_review,
                    COUNT(*) FILTER (WHERE rp.review_id IS NULL) AS missing_preprocessed,
                    COUNT(*) FILTER (WHERE aspect_counts.aspect_count IS NULL) AS missing_aspects
                FROM target t
                LEFT JOIN review_action_analysis raa
                  ON raa.review_id = t.review_id
                LEFT JOIN app_reviews ar
                  ON ar.platform_review_id = t.platform_review_id
                LEFT JOIN reviews_preprocessed rp
                  ON rp.review_id = t.review_id
                LEFT JOIN (
                    SELECT review_id, COUNT(*) AS aspect_count
                    FROM review_aspects
                    GROUP BY review_id
                ) aspect_counts
                  ON aspect_counts.review_id = t.review_id
                """
            ),
            {"target_date": target_date},
        ).one()
        metrics = {
            "missing_action_analysis": int(row.missing_action_analysis or 0),
            "missing_app_review": int(row.missing_app_review or 0),
            "missing_preprocessed": int(row.missing_preprocessed or 0),
            "missing_aspects": int(row.missing_aspects or 0),
        }
        missing_count = sum(metrics.values())
        return ValidationCheck(
            name="orphan_integrity",
            severity="failure",
            passed=missing_count == 0,
            metrics=metrics,
            message="Analyzed reviews have missing upstream analysis rows." if missing_count else None,
        )

    def _check_mart_freshness(self, session, target_date: date) -> ValidationCheck:
        analyzed_count = self._analyzed_count(session, target_date)
        row_counts = self._mart_row_counts(session, target_date)
        missing_tables = [
            table_name
            for table_name, row_count in row_counts.items()
            if analyzed_count > 0 and row_count == 0
        ]
        return ValidationCheck(
            name="mart_freshness",
            severity="failure",
            passed=not missing_tables,
            metrics={"analyzed_count": analyzed_count, "row_counts": row_counts},
            message=(
                f"Target-date mart rows are missing: {', '.join(missing_tables)}"
                if missing_tables
                else None
            ),
        )

    def _check_mart_count_consistency(self, session, target_date: date) -> ValidationCheck:
        expected_count = session.execute(
            text(
                """
                SELECT COUNT(*) AS count
                FROM review_master_index rmi
                JOIN app_reviews ar
                  ON ar.platform_review_id = rmi.platform_review_id
                WHERE DATE_TRUNC('day', rmi.review_created_at)::date = :target_date
                  AND rmi.processing_status::text = 'ANALYZED'
                  AND rmi.service_id IS NOT NULL
                  AND ar.platform_type IS NOT NULL
                """
            ),
            {"target_date": target_date},
        ).scalar_one()
        fact_total = session.execute(
            text(
                """
                SELECT COALESCE(SUM(total_review_cnt), 0) AS count
                FROM fact_service_review_daily
                WHERE date = :target_date
                """
            ),
            {"target_date": target_date},
        ).scalar_one()
        expected_count = int(expected_count or 0)
        fact_total = int(fact_total or 0)
        return ValidationCheck(
            name="mart_count_consistency",
            severity="failure",
            passed=expected_count == fact_total,
            metrics={
                "expected_analyzed_review_count": expected_count,
                "fact_service_review_daily_total": fact_total,
            },
            message="Mart summary count does not match analyzed upstream reviews."
            if expected_count != fact_total
            else None,
        )

    def _check_mart_quality(self, session, target_date: date) -> ValidationCheck:
        summary_violations = session.execute(
            text(
                """
                SELECT COUNT(*) AS count
                FROM fact_service_review_daily
                WHERE date = :target_date
                  AND (
                    COALESCE(total_review_cnt, 0) < 0
                    OR COALESCE(action_required_cnt, 0) < 0
                    OR COALESCE(attention_required_cnt, 0) < 0
                    OR COALESCE(pos_count, 0) < 0
                    OR COALESCE(neg_count, 0) < 0
                    OR (avg_rating IS NOT NULL AND (avg_rating < 1 OR avg_rating > 5))
                    OR (action_ratio IS NOT NULL AND (action_ratio < 0 OR action_ratio > 1))
                  )
                """
            ),
            {"target_date": target_date},
        ).scalar_one()
        aspect_violations = session.execute(
            text(
                """
                SELECT COUNT(*) AS count
                FROM fact_service_aspect_daily
                WHERE date = :target_date
                  AND (
                    COALESCE(mention_cnt, 0) < 0
                    OR (
                        avg_sentiment_score IS NOT NULL
                        AND (avg_sentiment_score < 0 OR avg_sentiment_score > 1)
                    )
                  )
                """
            ),
            {"target_date": target_date},
        ).scalar_one()
        radar_violations = session.execute(
            text(
                """
                SELECT COUNT(*) AS count
                FROM fact_category_radar_scores
                WHERE date = :target_date
                  AND (
                    COALESCE(review_cnt, 0) < 0
                    OR (
                        avg_sentiment_score IS NOT NULL
                        AND (avg_sentiment_score < 0 OR avg_sentiment_score > 1)
                    )
                  )
                """
            ),
            {"target_date": target_date},
        ).scalar_one()
        review_list_violations = session.execute(
            text(
                """
                SELECT COUNT(*) AS count
                FROM srv_daily_review_list
                WHERE date = :target_date
                  AND (
                    (rating IS NOT NULL AND (rating < 1 OR rating > 5))
                    OR (sentiment_score IS NOT NULL AND (sentiment_score < 0 OR sentiment_score > 1))
                    OR (confidence IS NOT NULL AND (confidence < 0 OR confidence > 1))
                  )
                """
            ),
            {"target_date": target_date},
        ).scalar_one()
        metrics = {
            "fact_service_review_daily": int(summary_violations or 0),
            "fact_service_aspect_daily": int(aspect_violations or 0),
            "fact_category_radar_scores": int(radar_violations or 0),
            "srv_daily_review_list": int(review_list_violations or 0),
        }
        violation_count = sum(metrics.values())
        return ValidationCheck(
            name="mart_quality",
            severity="failure",
            passed=violation_count == 0,
            metrics=metrics,
            message="Mart rows contain out-of-range quality metrics." if violation_count else None,
        )

    def _analyzed_count(self, session, target_date: date) -> int:
        return int(
            session.execute(
                text(
                    """
                    SELECT COUNT(*) AS count
                    FROM review_master_index
                    WHERE DATE_TRUNC('day', review_created_at)::date = :target_date
                      AND processing_status::text = 'ANALYZED'
                    """
                ),
                {"target_date": target_date},
            ).scalar_one()
            or 0
        )

    def _mart_row_counts(self, session, target_date: date) -> dict[str, int]:
        counts = {}
        for table_name in MART_TABLES:
            counts[table_name] = int(
                session.execute(
                    text(f"SELECT COUNT(*) AS count FROM {table_name} WHERE date = :target_date"),
                    {"target_date": target_date},
                ).scalar_one()
                or 0
            )
        return counts
