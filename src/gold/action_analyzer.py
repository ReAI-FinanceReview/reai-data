"""Gold Layer - Actionability & LLM Summary Engine

review_aspects + rating을 바탕으로 조치 필요 여부를 판별하고
LLM으로 1문장 요약을 생성하여 review_action_analysis에 적재.

판별 로직:
    is_attention_required (규칙 기반):
        Case A: rating >= 4 AND avg(sentiment) < 0.4  → 별점 높지만 부정 내용
        Case B: rating <= 2 AND avg(sentiment) > 0.6  → 별점 낮지만 긍정 내용

    is_action_required (Snorkel LFs + MajorityLabelVoter):
        LF_bug_keyword    : "버그/팅김/오류/에러/강제종료" 포함
        LF_request_keyword: "해달라/고쳐줘/개선해/부탁" 포함
        LF_low_rating     : rating <= 2

    review_summary (LLM):
        OpenAI gpt-4o-mini로 1문장 추출적 요약

Usage (standalone):
    analyzer = GoldActionAnalyzer()
    analyzer.process_batch(batch_size=100)

Usage (via orchestrator):
    success = analyzer.process(session, review_id)
"""

from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional
from uuid import UUID

try:
    from snorkel.labeling import labeling_function, LabelingFunction
    from snorkel.labeling.model import MajorityLabelVoter
    import numpy as np
    SNORKEL_AVAILABLE = True
except ImportError:
    SNORKEL_AVAILABLE = False

try:
    from openai import OpenAI
    from openai import APIError, RateLimitError
    OPENAI_AVAILABLE = True
except ImportError:
    OPENAI_AVAILABLE = False
    OpenAI = None  # type: ignore
    APIError = Exception  # type: ignore
    RateLimitError = Exception  # type: ignore

try:
    from dotenv import load_dotenv
    _env_path = Path(__file__).resolve().parents[2] / ".env"
    if _env_path.exists():
        load_dotenv(_env_path)
except ImportError:
    pass

from src.models.enums import AnalysisStatusType, ProcessingStatusType
from src.models.llm_analysis_log import LLMAnalysisLog
from src.models.review_action_analysis import ReviewActionAnalysis
from src.models.review_aspects import ReviewAspect
from src.models.review_master_index import ReviewMasterIndex
from src.models.review_preprocessed import ReviewPreprocessed
from src.utils.db_connector import DatabaseConnector
from src.utils.logger import get_logger


# ──────────────────────────────────────────────
# Snorkel label constants
# ──────────────────────────────────────────────
ABSTAIN = -1
ACTION_NOT_REQUIRED = 0
ACTION_REQUIRED = 1

# ──────────────────────────────────────────────
# Labeling Functions
# ──────────────────────────────────────────────

def _lf_bug_keyword(text: str) -> int:
    """버그/오류 키워드가 포함된 리뷰 → 조치 필요."""
    _BUG_KEYWORDS = {"버그", "팅김", "오류", "에러", "강제종료", "먹통", "충돌", "다운"}
    if any(kw in text for kw in _BUG_KEYWORDS):
        return ACTION_REQUIRED
    return ABSTAIN


def _lf_request_keyword(text: str) -> int:
    """개선/요청 키워드가 포함된 리뷰 → 조치 필요."""
    _REQUEST_KEYWORDS = {"해달라", "고쳐줘", "개선해", "부탁", "요청", "수정해", "바꿔줘", "추가해"}
    if any(kw in text for kw in _REQUEST_KEYWORDS):
        return ACTION_REQUIRED
    return ABSTAIN


def _lf_low_rating(rating: int) -> int:
    """별점 2 이하 → 조치 필요."""
    if rating <= 2:
        return ACTION_REQUIRED
    return ABSTAIN


def _apply_lfs(text: str, rating: int) -> tuple[int, float, str]:
    """
    LF 결과를 MajorityLabelVoter로 집계.

    Returns:
        (label, confidence, trigger_reason)
        label: ACTION_REQUIRED(1) 또는 ACTION_NOT_REQUIRED(0)
        confidence: 0.0 ~ 1.0
        trigger_reason: 발화 LF 목록
    """
    votes = {
        "LF_bug_keyword": _lf_bug_keyword(text),
        "LF_request_keyword": _lf_request_keyword(text),
        "LF_low_rating": _lf_low_rating(rating),
    }

    fired = [name for name, v in votes.items() if v != ABSTAIN]
    positive = sum(1 for v in votes.values() if v == ACTION_REQUIRED)
    total_voted = len(fired)

    if total_voted == 0:
        return ACTION_NOT_REQUIRED, 0.5, ""

    confidence = positive / total_voted
    label = ACTION_REQUIRED if confidence >= 0.5 else ACTION_NOT_REQUIRED
    trigger_reason = ", ".join(fired) if fired else ""

    return label, confidence, trigger_reason


# ──────────────────────────────────────────────
# Main Analyzer Class
# ──────────────────────────────────────────────

class GoldActionAnalyzer:
    """review_aspects + rating → review_action_analysis 적재 (Gold Layer).

    Orchestrator 단일 건 처리와 standalone 배치 처리 모두 지원.
    """

    _LLM_MODEL = "gpt-4o-mini"
    _MAX_RETRIES = 3
    _RETRY_BACKOFF = 2.0

    def __init__(self, config_path: str = "config/crawler_config.yml"):
        self.logger = get_logger(__name__)
        self.db_connector = DatabaseConnector(config_path)
        self._llm = self._init_llm_client()

    # ──────────────────────────────────────────
    # Public API
    # ──────────────────────────────────────────

    def process(self, session, review_id: UUID) -> bool:
        """단일 review_id에 대해 Actionability 분석 후 DB 적재.

        Orchestrator에서 호출. 세션 관리는 호출자 책임.

        Returns:
            True: 성공(신규 적재 또는 이미 존재)
            False: 실패
        """
        if self._is_already_analyzed(session, review_id):
            return True

        try:
            record = self._build_record(session, review_id)
        except Exception as exc:
            self.logger.error(f"[{review_id}] ActionAnalyzer 실패: {exc}")
            return False

        if record is None:
            return True  # 데이터 부족 → skip

        session.merge(record)
        return True

    def process_batch(
        self,
        batch_size: int = 100,
        limit: Optional[int] = None,
    ) -> int:
        """standalone 배치 실행 — ANALYZED 아닌 review_action_analysis 미적재 건 처리.

        Returns:
            처리(성공) 건수
        """
        session = self.db_connector.get_session()
        processed = 0
        try:
            review_ids = self._fetch_pending_ids(session, limit)
            self.logger.info(f"Action analysis 대상: {len(review_ids)}건")

            for i in range(0, len(review_ids), batch_size):
                batch = review_ids[i : i + batch_size]
                for rid in batch:
                    ok = self.process(session, rid)
                    if ok:
                        processed += 1
                session.commit()
                self.logger.info(
                    f"진행: {min(i + batch_size, len(review_ids))}/{len(review_ids)}"
                )
        except Exception as exc:
            session.rollback()
            self.logger.error(f"process_batch 오류: {exc}")
            raise
        finally:
            session.close()

        return processed

    # ──────────────────────────────────────────
    # Core Logic
    # ──────────────────────────────────────────

    def _build_record(
        self, session, review_id: UUID
    ) -> Optional[ReviewActionAnalysis]:
        """분석 수행 후 ReviewActionAnalysis 객체 반환."""
        # 1. rating 조회 (app_reviews via review_master_index.platform_review_id)
        from src.models.review import Review as AppReview
        rmi = session.get(ReviewMasterIndex, review_id)
        if rmi is None:
            self.logger.warning(f"[{review_id}] ReviewMasterIndex 없음 — skip")
            return None

        app_review = (
            session.query(AppReview)
            .filter_by(platform_review_id=rmi.platform_review_id)
            .first()
        )
        rating: int = app_review.rating if app_review else 3  # 기본 중립

        # 2. review_aspects 평균 감성 점수 조회
        aspects: List[ReviewAspect] = (
            session.query(ReviewAspect)
            .filter(ReviewAspect.review_id == review_id)
            .all()
        )

        preprocessed = session.get(ReviewPreprocessed, review_id)
        if preprocessed is None or not preprocessed.refined_text:
            self.logger.warning(f"[{review_id}] refined_text 없음 — skip")
            return None

        text = preprocessed.refined_text
        avg_sentiment = (
            sum(a.sentiment_score for a in aspects if a.sentiment_score is not None)
            / len(aspects)
            if aspects
            else 0.5
        )

        # 3. is_attention_required (별점-감성 불일치)
        is_attention = self._calc_attention(rating, avg_sentiment)

        # 4. is_action_required (Snorkel LFs)
        label, confidence, trigger_reason = _apply_lfs(text, rating)
        is_action = label == ACTION_REQUIRED

        # 5. review_summary (LLM)
        summary = self._generate_summary(session, review_id, text)

        return ReviewActionAnalysis(
            review_id=review_id,
            is_action_required=is_action,
            action_confidence_score=confidence,
            trigger_reason=trigger_reason or None,
            is_attention_required=is_attention,
            is_verified=False,
            review_summary=summary,
            analyzed_at=datetime.now(timezone.utc),
        )

    def _calc_attention(self, rating: int, avg_sentiment: float) -> bool:
        """별점-감성 불일치 감지."""
        if rating >= 4 and avg_sentiment < 0.4:
            return True  # Case A: 높은 별점, 부정 텍스트
        if rating <= 2 and avg_sentiment > 0.6:
            return True  # Case B: 낮은 별점, 긍정 텍스트
        return False

    # ──────────────────────────────────────────
    # LLM Summary
    # ──────────────────────────────────────────

    def _generate_summary(
        self, session, review_id: UUID, text: str
    ) -> Optional[str]:
        """OpenAI로 1문장 요약 생성 + LLM 로그 기록."""
        if not self._llm:
            return None

        prompt = (
            "다음 금융 앱 리뷰를 한국어 1문장으로 핵심만 요약하세요. "
            "마침표로 끝내세요.\n\n"
            f"리뷰: {text[:500]}"
        )

        log = LLMAnalysisLog(
            source_table="review_action_analysis",
            source_record_id=str(review_id),
            model_name=self._LLM_MODEL,
            params=json.dumps({"max_tokens": 100, "temperature": 0.3}),
            status=AnalysisStatusType.PROCESSING,
        )
        session.add(log)
        session.flush()  # log.id 확보

        summary: Optional[str] = None
        for attempt in range(1, self._MAX_RETRIES + 1):
            try:
                resp = self._llm.chat.completions.create(
                    model=self._LLM_MODEL,
                    messages=[{"role": "user", "content": prompt}],
                    max_tokens=100,
                    temperature=0.3,
                )
                summary = resp.choices[0].message.content.strip()
                log.status = AnalysisStatusType.SUCCESS
                log.result_payload = {"summary": summary}
                log.processed_at = datetime.now(timezone.utc)
                break
            except RateLimitError as exc:
                self.logger.warning(
                    f"[{review_id}] LLM RateLimit (시도 {attempt}): {exc}"
                )
            except APIError as exc:
                self.logger.error(f"[{review_id}] LLM APIError (시도 {attempt}): {exc}")
                if getattr(exc, "status_code", None) and exc.status_code < 500:
                    break
            except Exception as exc:
                self.logger.error(f"[{review_id}] LLM 오류 (시도 {attempt}): {exc}")
                break

            if attempt < self._MAX_RETRIES:
                time.sleep(self._RETRY_BACKOFF * attempt)

        if summary is None:
            log.status = AnalysisStatusType.FAILED
            log.error_message = "max retries exceeded"

        return summary

    # ──────────────────────────────────────────
    # Helpers
    # ──────────────────────────────────────────

    def _is_already_analyzed(self, session, review_id: UUID) -> bool:
        row = (
            session.query(ReviewActionAnalysis.review_id)
            .filter(ReviewActionAnalysis.review_id == review_id)
            .first()
        )
        if row:
            self.logger.debug(f"[{review_id}] 이미 분석됨 — skip")
        return row is not None

    def _fetch_pending_ids(self, session, limit: Optional[int]) -> List[UUID]:
        """review_action_analysis에 없는 CLEANED/ANALYZED review_id 조회."""
        from sqlalchemy import not_, exists

        subq = exists().where(
            ReviewActionAnalysis.review_id == ReviewMasterIndex.review_id
        )
        q = (
            session.query(ReviewMasterIndex.review_id)
            .filter(
                ReviewMasterIndex.processing_status.in_(
                    [ProcessingStatusType.CLEANED, ProcessingStatusType.ANALYZED]
                )
            )
            .filter(not_(subq))
        )
        if limit:
            q = q.limit(limit)
        return [row.review_id for row in q.all()]

    def _init_llm_client(self):
        if not OPENAI_AVAILABLE:
            self.logger.warning("openai 패키지 없음 — LLM summary 비활성화")
            return None
        api_key = os.getenv("OPENAI_API_KEY")
        if not api_key:
            self.logger.warning("OPENAI_API_KEY 미설정 — LLM summary 비활성화")
            return None
        base_url = os.getenv("OPENAI_BASE_URL")
        try:
            return OpenAI(api_key=api_key, base_url=base_url)
        except Exception as exc:
            self.logger.error(f"OpenAI 클라이언트 초기화 실패: {exc}")
            return None
