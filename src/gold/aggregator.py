"""Gold Layer - Aggregator

ANALYZED 상태의 리뷰를 집계하여 Fact 테이블과 서빙 마트를 갱신합니다.

집계 대상 테이블:
  - fact_service_review_daily   : 일별 서비스 리뷰 통계
  - fact_service_aspect_daily   : 일별 서비스 애스펙트 통계
  - fact_category_radar_scores  : 카테고리별 레이더 점수
  - srv_daily_review_list       : 비정규화 와이드 테이블 (대시보드용)

Usage:
    aggregator = GoldAggregator()
    aggregator.run(target_date=date.today())
"""

from __future__ import annotations

from datetime import date, timedelta
from typing import List, Optional

from sqlalchemy import text

from src.utils.db_connector import DatabaseConnector
from src.utils.logger import get_logger


class GoldAggregator:
    """Gold Layer 집계기.

    Orchestrator 완료 후 target_date 기준으로 Fact 테이블을 UPSERT합니다.
    """

    def __init__(self, config_path: str = "config/crawler_config.yml"):
        self.logger = get_logger(__name__)
        self.db_connector = DatabaseConnector(config_path)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def run(self, target_date: Optional[date] = None) -> dict:
        """target_date 기준 집계 실행.

        Args:
            target_date: 집계할 날짜. None이면 오늘 날짜 사용.

        Returns:
            {"date": str, "tables_updated": list[str]}
        """
        if target_date is None:
            target_date = date.today()

        self.logger.info(f"Gold Aggregator 시작: target_date={target_date}")

        session = self.db_connector.get_session()
        updated = []
        try:
            self._upsert_fact_service_review_daily(session, target_date)
            updated.append("fact_service_review_daily")

            self._upsert_fact_service_aspect_daily(session, target_date)
            updated.append("fact_service_aspect_daily")

            self._upsert_fact_category_radar_scores(session, target_date)
            updated.append("fact_category_radar_scores")

            self._upsert_srv_daily_review_list(session, target_date)
            updated.append("srv_daily_review_list")

            session.commit()
            self.logger.info(f"Gold Aggregator 완료: date={target_date}, tables={updated}")
            return {"date": str(target_date), "tables_updated": updated}

        except Exception:
            session.rollback()
            self.logger.exception(f"Gold Aggregator 실패: target_date={target_date}")
            raise
        finally:
            session.close()

    def run_all(self) -> dict:
        """ANALYZED 레코드의 모든 distinct 날짜를 집계 (드레인 모드).

        gold_analyze가 날짜 무관하게 전체 드레인하기 때문에, 재시도로 이전 날짜
        리뷰가 ANALYZED 되더라도 해당 날짜 집계가 누락되지 않도록 보장합니다.

        날짜별로 독립 커밋하여 중간 실패 시에도 성공한 날짜의 진행 상황을 보존합니다.

        Returns:
            {"dates": list[str], "failed_dates": list[str], "tables_updated": list[str]}
        """
        session = self.db_connector.get_session()
        updated_dates: List[str] = []
        failed_dates: List[str] = []
        try:
            dates = self._fetch_analyzed_dates(session)
            if not dates:
                self.logger.info("Gold Aggregator run_all: 집계 대상 날짜 없음")
                return {"dates": [], "failed_dates": [], "tables_updated": []}

            self.logger.info(f"Gold Aggregator run_all: {len(dates)}개 날짜 집계 시작")
            for d in dates:
                try:
                    self._upsert_fact_service_review_daily(session, d)
                    self._upsert_fact_service_aspect_daily(session, d)
                    self._upsert_fact_category_radar_scores(session, d)
                    self._upsert_srv_daily_review_list(session, d)
                    session.commit()
                    updated_dates.append(str(d))
                except Exception:
                    session.rollback()
                    self.logger.exception(f"Gold Aggregator run_all 날짜 집계 실패, 스킵: date={d}")
                    failed_dates.append(str(d))

            self.logger.info(
                f"Gold Aggregator run_all 완료: updated={updated_dates}, failed={failed_dates}"
            )
            if failed_dates:
                raise RuntimeError(
                    f"Gold Aggregator run_all: 일부 날짜 집계 실패 {failed_dates} "
                    f"(성공: {updated_dates})"
                )
            return {
                "dates": updated_dates,
                "failed_dates": failed_dates,
                "tables_updated": [
                    "fact_service_review_daily",
                    "fact_service_aspect_daily",
                    "fact_category_radar_scores",
                    "srv_daily_review_list",
                ],
            }
        finally:
            session.close()

    # ------------------------------------------------------------------
    # Per-table UPSERT helpers
    # ------------------------------------------------------------------

    def _fetch_analyzed_dates(self, session) -> List[date]:
        """ANALYZED 레코드가 존재하는 모든 distinct 날짜 조회."""
        sql = text("""
            SELECT DISTINCT DATE_TRUNC('day', review_created_at)::date AS d
            FROM review_master_index
            WHERE processing_status = 'ANALYZED'
              AND review_created_at IS NOT NULL
            ORDER BY d
        """)
        return [row.d for row in session.execute(sql)]

    def _ensure_partition(self, session, target_date: date) -> None:
        """srv_daily_review_list 파티션이 없으면 생성.

        PostgreSQL의 FOR VALUES FROM ... TO ... 절은 bind parameter를 허용하지 않으므로
        날짜를 ISO 문자열 리터럴로 직접 삽입. partition_name은 date.strftime으로
        생성되어 숫자·언더스코어만 포함하므로 안전함.
        """
        partition_name = f"srv_daily_review_list_{target_date.strftime('%Y_%m_%d')}"
        next_date = target_date + timedelta(days=1)
        ddl = text(
            f"CREATE TABLE IF NOT EXISTS public.{partition_name} "
            f"PARTITION OF public.srv_daily_review_list "
            f"FOR VALUES FROM ('{target_date.isoformat()}') TO ('{next_date.isoformat()}')"
        )
        session.execute(ddl)
        self.logger.debug(f"파티션 확인/생성: {partition_name}")

    def _upsert_fact_service_review_daily(self, session, target_date: date) -> None:
        """fact_service_review_daily UPSERT."""
        sql = text("""
            INSERT INTO fact_service_review_daily
                (date, service_id, platform_type,
                 total_review_cnt, action_required_cnt, attention_required_cnt,
                 avg_rating, pos_count, neg_count, action_ratio)
            SELECT
                DATE_TRUNC('day', rmi.review_created_at)::date AS date,
                rmi.service_id,
                ar.platform_type,
                COUNT(*)                                        AS total_review_cnt,
                SUM(CASE WHEN raa.is_action_required    THEN 1 ELSE 0 END) AS action_required_cnt,
                SUM(CASE WHEN raa.is_attention_required THEN 1 ELSE 0 END) AS attention_required_cnt,
                AVG(ar.rating)                                  AS avg_rating,
                COUNT(DISTINCT CASE WHEN avg_sent.avg_score >= 0.5 THEN rmi.review_id END) AS pos_count,
                COUNT(DISTINCT CASE WHEN avg_sent.avg_score <  0.5 THEN rmi.review_id END) AS neg_count,
                ROUND(
                    SUM(CASE WHEN raa.is_action_required THEN 1 ELSE 0 END)::FLOAT
                    / NULLIF(COUNT(*), 0), 4
                ) AS action_ratio
            FROM review_master_index rmi
            JOIN review_action_analysis raa USING (review_id)
            JOIN app_reviews ar ON ar.platform_review_id = rmi.platform_review_id
            LEFT JOIN (
                SELECT review_id, AVG(sentiment_score) AS avg_score
                FROM review_aspects GROUP BY review_id
            ) avg_sent ON avg_sent.review_id = rmi.review_id
            WHERE rmi.processing_status = 'ANALYZED'
              AND DATE_TRUNC('day', rmi.review_created_at)::date = :target_date
              AND rmi.service_id IS NOT NULL
              AND ar.platform_type IS NOT NULL
            GROUP BY 1, 2, 3
            ON CONFLICT (date, service_id, platform_type)
            DO UPDATE SET
                total_review_cnt       = EXCLUDED.total_review_cnt,
                action_required_cnt    = EXCLUDED.action_required_cnt,
                attention_required_cnt = EXCLUDED.attention_required_cnt,
                avg_rating             = EXCLUDED.avg_rating,
                pos_count              = EXCLUDED.pos_count,
                neg_count              = EXCLUDED.neg_count,
                action_ratio           = EXCLUDED.action_ratio
        """)
        session.execute(sql, {"target_date": target_date})
        self.logger.debug(f"fact_service_review_daily UPSERT 완료: {target_date}")

    def _upsert_fact_service_aspect_daily(self, session, target_date: date) -> None:
        """fact_service_aspect_daily UPSERT."""
        sql = text("""
            INSERT INTO fact_service_aspect_daily
                (date, service_id, keyword, mention_cnt, avg_sentiment_score)
            SELECT
                DATE_TRUNC('day', rmi.review_created_at)::date AS date,
                rmi.service_id,
                ra.keyword,
                COUNT(*)                                        AS mention_cnt,
                AVG(ra.sentiment_score)                         AS avg_sentiment_score
            FROM review_aspects ra
            JOIN review_master_index rmi USING (review_id)
            WHERE rmi.processing_status = 'ANALYZED'
              AND DATE_TRUNC('day', rmi.review_created_at)::date = :target_date
              AND rmi.service_id IS NOT NULL
              AND ra.keyword IS NOT NULL
            GROUP BY 1, 2, 3
            ON CONFLICT (date, service_id, keyword)
            DO UPDATE SET
                mention_cnt         = EXCLUDED.mention_cnt,
                avg_sentiment_score = EXCLUDED.avg_sentiment_score
        """)
        session.execute(sql, {"target_date": target_date})
        self.logger.debug(f"fact_service_aspect_daily UPSERT 완료: {target_date}")

    def _upsert_fact_category_radar_scores(self, session, target_date: date) -> None:
        """fact_category_radar_scores UPSERT."""
        sql = text("""
            INSERT INTO fact_category_radar_scores
                (date, service_id, category_type, avg_sentiment_score, review_cnt)
            SELECT
                DATE_TRUNC('day', rmi.review_created_at)::date AS date,
                rmi.service_id,
                ra.category::category_type                     AS category_type,
                AVG(ra.sentiment_score)                        AS avg_sentiment_score,
                COUNT(DISTINCT ra.review_id)                   AS review_cnt
            FROM review_aspects ra
            JOIN review_master_index rmi USING (review_id)
            WHERE rmi.processing_status = 'ANALYZED'
              AND DATE_TRUNC('day', rmi.review_created_at)::date = :target_date
              AND rmi.service_id IS NOT NULL
              AND ra.category IN ('USABILITY', 'STABILITY', 'DESIGN', 'CUSTOMER_SUPPORT', 'SPEED')
            GROUP BY 1, 2, 3
            ON CONFLICT (date, service_id, category_type)
            DO UPDATE SET
                avg_sentiment_score = EXCLUDED.avg_sentiment_score,
                review_cnt          = EXCLUDED.review_cnt
        """)
        session.execute(sql, {"target_date": target_date})
        self.logger.debug(f"fact_category_radar_scores UPSERT 완료: {target_date}")

    def _upsert_srv_daily_review_list(self, session, target_date: date) -> None:
        """srv_daily_review_list UPSERT (비정규화 와이드 테이블)."""
        self._ensure_partition(session, target_date)
        sql = text("""
            INSERT INTO srv_daily_review_list (
                review_id, date, service_id, refined_text, review_summary,
                rating, reviewed_at, sentiment_score, is_action_required,
                is_attention_required, assigned_dept, keyword, confidence
            )
            SELECT
                rmi.review_id,
                DATE_TRUNC('day', rmi.review_created_at)::date AS date,
                rmi.service_id,
                rp.refined_text,
                raa.review_summary,
                ar.rating,
                rmi.review_created_at                          AS reviewed_at,
                (SELECT AVG(sentiment_score) FROM review_aspects
                 WHERE review_id = rmi.review_id)              AS sentiment_score,
                raa.is_action_required,
                raa.is_attention_required,
                rvs.assigned_dept,
                ARRAY(SELECT DISTINCT keyword FROM review_aspects
                      WHERE review_id = rmi.review_id
                        AND keyword IS NOT NULL)               AS keyword,
                rvs.confidence
            FROM review_master_index rmi
            JOIN review_action_analysis raa USING (review_id)
            JOIN app_reviews ar ON ar.platform_review_id = rmi.platform_review_id
            LEFT JOIN reviews_preprocessed rp USING (review_id)
            LEFT JOIN (
                SELECT DISTINCT ON (review_id) review_id, assigned_dept, confidence
                FROM reviews_assigned ORDER BY review_id, created_at DESC
            ) rvs USING (review_id)
            WHERE rmi.processing_status = 'ANALYZED'
              AND DATE_TRUNC('day', rmi.review_created_at)::date = :target_date
            ON CONFLICT (review_id, date)
            DO UPDATE SET
                review_summary        = EXCLUDED.review_summary,
                sentiment_score       = EXCLUDED.sentiment_score,
                is_action_required    = EXCLUDED.is_action_required,
                is_attention_required = EXCLUDED.is_attention_required,
                assigned_dept         = EXCLUDED.assigned_dept,
                keyword               = EXCLUDED.keyword,
                confidence            = EXCLUDED.confidence
        """)
        session.execute(sql, {"target_date": target_date})
        self.logger.debug(f"srv_daily_review_list UPSERT 완료: {target_date}")
