"""Gold Layer - Orchestrator

CLEANED 상태의 review_master_index 레코드를 배치로 가져와
Gold 분석 모듈(embedding → ABSA → action)을 순차 실행한 뒤
processing_status를 ANALYZED / FAILED로 갱신합니다.

재시도 조건: processing_status = FAILED AND retry_count < 3

Usage:
    orchestrator = GoldOrchestrator()
    orchestrator.run(batch_size=100)
"""

from __future__ import annotations

import traceback
from datetime import datetime, timezone
from typing import List, Optional
from uuid import UUID

from src.models.enums import ProcessingStatusType
from src.models.review_master_index import ReviewMasterIndex
from src.utils.db_connector import DatabaseConnector
from src.utils.logger import get_logger

_MAX_RETRY = 3


class GoldOrchestrator:
    """Gold Layer 파이프라인 오케스트레이터.

    CLEANED 레코드를 embedding → ABSA → action analysis 순서로 처리하고
    처리 결과에 따라 processing_status를 ANALYZED / FAILED로 갱신합니다.
    """

    def __init__(self, config_path: str = "config/crawler_config.yml"):
        self.logger = get_logger(__name__)
        self.db_connector = DatabaseConnector(config_path)

        # 분석 모듈은 지연 임포트로 의존성 오류를 격리
        from src.gold.embedding_generator import GoldEmbeddingGenerator
        from src.gold.absa_analyzer import GoldABSAAnalyzer
        from src.gold.action_analyzer import GoldActionAnalyzer

        self._embedding = GoldEmbeddingGenerator(config_path=config_path)
        self._absa = GoldABSAAnalyzer(config_path=config_path)
        self._action = GoldActionAnalyzer(config_path=config_path)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def run(
        self,
        batch_size: int = 100,
        limit: Optional[int] = None,
    ) -> dict:
        """CLEANED / 재시도 대상 레코드를 배치 처리.

        Args:
            batch_size: 한 번에 처리할 레코드 수.
            limit: 처리할 최대 레코드 수 (None = 무제한).

        Returns:
            {"total": int, "analyzed": int, "failed": int}
        """
        session = self.db_connector.get_session()
        try:
            review_ids = self._fetch_pending_ids(session, limit)
            if not review_ids:
                self.logger.info("Gold Orchestrator: 처리 대상 리뷰 없음")
                return {"total": 0, "analyzed": 0, "failed": 0}

            self.logger.info(f"Gold Orchestrator: {len(review_ids)}건 처리 시작")
            analyzed, failed = 0, 0

            for i in range(0, len(review_ids), batch_size):
                chunk = review_ids[i : i + batch_size]
                for review_id in chunk:
                    ok = self._process_one(session, review_id)
                    if ok:
                        analyzed += 1
                    else:
                        failed += 1

                session.commit()
                self.logger.info(
                    f"진행 중: {min(i + batch_size, len(review_ids))}/{len(review_ids)} "
                    f"(analyzed={analyzed}, failed={failed})"
                )

            self.logger.info(
                f"Gold Orchestrator 완료: total={len(review_ids)}, "
                f"analyzed={analyzed}, failed={failed}"
            )
            return {"total": len(review_ids), "analyzed": analyzed, "failed": failed}

        except Exception:
            session.rollback()
            self.logger.exception("Gold Orchestrator 배치 처리 중 예외 발생")
            raise
        finally:
            session.close()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _fetch_pending_ids(
        self, session, limit: Optional[int]
    ) -> List[UUID]:
        """처리 대상 review_id 목록 조회.

        대상:
          1) processing_status = CLEANED
          2) processing_status = FAILED AND retry_count < _MAX_RETRY
        """
        query = session.query(ReviewMasterIndex.review_id).filter(
            (ReviewMasterIndex.processing_status == ProcessingStatusType.CLEANED)
            | (
                (ReviewMasterIndex.processing_status == ProcessingStatusType.FAILED)
                & (ReviewMasterIndex.retry_count < _MAX_RETRY)
            )
        )
        if limit:
            query = query.limit(limit)
        return [row.review_id for row in query.all()]

    def _process_one(self, session, review_id: UUID) -> bool:
        """단일 review_id에 대해 전체 Gold 분석 파이프라인 실행.

        Returns:
            True  - 전 모듈 성공 → processing_status = ANALYZED
            False - 오류 발생   → processing_status = FAILED, retry_count++
        """
        try:
            ok_embed = self._embedding.process(session, review_id)
            if not ok_embed:
                raise RuntimeError("EmbeddingGenerator.process() returned False")

            ok_absa = self._absa.process(session, review_id)
            if not ok_absa:
                raise RuntimeError("GoldABSAAnalyzer.process() returned False")

            ok_action = self._action.process(session, review_id)
            if not ok_action:
                raise RuntimeError("GoldActionAnalyzer.process() returned False")

            self._update_status(
                session,
                review_id,
                status=ProcessingStatusType.ANALYZED,
                error_message=None,
            )
            return True

        except Exception as exc:
            err_msg = f"{type(exc).__name__}: {exc}\n{traceback.format_exc()}"
            self.logger.warning(f"[{review_id}] Gold 분석 실패: {err_msg[:200]}")
            self._update_status(
                session,
                review_id,
                status=ProcessingStatusType.FAILED,
                error_message=err_msg[:2000],
                increment_retry=True,
            )
            return False

    def _update_status(
        self,
        session,
        review_id: UUID,
        status: ProcessingStatusType,
        error_message: Optional[str],
        increment_retry: bool = False,
    ) -> None:
        record = session.get(ReviewMasterIndex, review_id)
        if record is None:
            self.logger.error(f"[{review_id}] ReviewMasterIndex 레코드 없음 — 상태 갱신 불가")
            return

        record.processing_status = status
        record.error_message = error_message
        if increment_retry:
            record.retry_count = (record.retry_count or 0) + 1
