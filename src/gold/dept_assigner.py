"""Gold Layer - 부서 배정 (Department Assignment)

ANALYZED 상태 리뷰를 organizations 의 담당 부서에 배정하고 reviews_assigned 에
적재한다. 배정기는 두 구현이 같은 계약을 공유한다.

- ``RuleBasedAssigner``: review_aspects.keyword[] 와 organizations.keywords[] 의
  교집합 개수로 배정. 이슈 #40 원 스펙이며 LLM 배정의 평가 베이스라인이다.
- ``LLMAssigner``: 같은 후보를 받아 LLM 이 하나를 고른다.

배정 단위는 리뷰 1건 = reviews_assigned 1행이고, 재실행 시 review_id 기준
UPSERT 한다(리비전 20260813_0002 가 UNIQUE 제약을 건다).

assigned_dept 에는 org_name 이 아니라 **org_id** 를 넣으며, 하위 조직을 배정할
때는 상위 경로를 함께 담는다. org_id 는 하이픈 구분 계층이다: ``1``, ``1-1``,
``10-5``.

Usage:
    assigner = RuleBasedAssigner()
    assigner.assign_batch(batch_size=100)
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from typing import Dict, List, Optional, Protocol, Sequence
from uuid import UUID

from pydantic import BaseModel
from sqlalchemy import text

from src.models.enums import AnalysisStatusType
from src.models.llm_analysis_log import LLMAnalysisLog
from src.utils.db_connector import DatabaseConnector
from src.utils.logger import get_logger

try:
    from openai import APIError, OpenAI, RateLimitError

    OPENAI_AVAILABLE = True
except ImportError:  # pragma: no cover - 패키지 미설치 환경
    OpenAI = None  # type: ignore[assignment]
    APIError = RateLimitError = Exception  # type: ignore[misc,assignment]
    OPENAI_AVAILABLE = False

UNCLASSIFIED = "미분류"

# DB 접속 설정 파일. 다른 gold 모듈과 같은 기본값이다.
DEFAULT_CONFIG_PATH = "config/crawler_config.yml"

# LLM 배정기가 프롬프트에 넣을 후보 수. 규칙 배정기도 같은 후보 집합을 쓴다.
DEFAULT_TOP_K = 10


# ----------------------------------------------------------------------
# 공통 타입
# ----------------------------------------------------------------------


@dataclass
class OrgCandidate:
    """배정 후보 부서 하나."""

    org_id: str
    org_name: str
    role_responsibility: Optional[str]
    keywords: List[str] = field(default_factory=list)
    matched: List[str] = field(default_factory=list)

    @property
    def score(self) -> int:
        """교집합 개수. 동점 처리는 호출부에서 계층 깊이로 판단한다."""
        return len(self.matched)


@dataclass
class Assignment:
    """배정 결과. 두 배정기가 공유하는 출력 타입."""

    review_id: UUID
    assigned_dept: List[str]
    assignment_reason: str
    confidence: float
    is_failed: bool = False
    try_number: int = 1
    # 어느 배정기가 만든 행인지. 이 값이 없으면 규칙/LLM 결과가 서로를 덮어써서
    # dept_eval 이 비교할 대상이 남지 않는다 (리비전 20260813_0002).
    assigner: str = "rule"

    def as_dict(self) -> dict:
        return {
            "review_id": str(self.review_id),
            "assigner": self.assigner,
            "assigned_dept": self.assigned_dept,
            "assignment_reason": self.assignment_reason,
            "confidence": self.confidence,
            "is_failed": self.is_failed,
            "try_number": self.try_number,
        }


class Assigner(Protocol):
    """배정기 계약. 규칙/LLM 구현이 서로 교체 가능해야 평가가 성립한다."""

    def assign(self, session, review_id: UUID) -> Assignment: ...

    def assign_batch(
        self,
        batch_size: int = 100,
        limit: Optional[int] = None,
        target_date: Optional[date] = None,
        reassign: bool = False,
    ) -> dict: ...


# ----------------------------------------------------------------------
# 계층 경로 유틸 — 두 배정기가 공유한다
# ----------------------------------------------------------------------


def expand_org_path(org_id: str) -> List[str]:
    """org_id 를 상위 경로를 포함한 배열로 펼친다.

    >>> expand_org_path("1-1-2")
    ['1', '1-1', '1-1-2']
    """
    parts = org_id.split("-")
    return ["-".join(parts[: i + 1]) for i in range(len(parts))]


def org_depth(org_id: str) -> int:
    """계층 깊이. ``1`` 은 1, ``1-1`` 은 2."""
    return len(org_id.split("-"))


# ----------------------------------------------------------------------
# 후보 검색
# ----------------------------------------------------------------------


class OrgCandidateRetriever:
    """리뷰 키워드로 배정 후보 부서를 좁힌다.

    임베딩 유사도는 쓰지 않는다. 리뷰(구어체·짧음)와 부서 설명(문어체·김)은
    비대칭 비교라 신뢰도가 낮고, ABSA 의 같은 방식 폴백이 placeholder 앵커로
    방치되어 한 번도 동작하지 않았다(#93). 라벨이 쌓이면 리뷰↔리뷰 kNN 으로
    전환한다.
    """

    def __init__(self, top_k: int = DEFAULT_TOP_K):
        self.top_k = top_k
        self.logger = get_logger(__name__)
        self._orgs: Optional[List[OrgCandidate]] = None

    def load_organizations(self, session) -> List[OrgCandidate]:
        """organizations 전량을 메모리에 적재한다 (114행, 배치당 1회)."""
        if self._orgs is not None:
            return self._orgs

        rows = session.execute(
            text(
                "SELECT org_id, org_name, role_responsibility, keywords "
                "FROM organizations "
                "ORDER BY string_to_array(org_id, '-')::int[]"
            )
        ).fetchall()

        self._orgs = [
            OrgCandidate(
                org_id=row[0],
                org_name=row[1] or "",
                role_responsibility=row[2],
                keywords=list(row[3] or []),
            )
            for row in rows
        ]
        if not self._orgs:
            self.logger.warning(
                "organizations 테이블이 비어 있음 — bootstrap 로드 순서 확인 필요 (#95)"
            )
        return self._orgs

    def fetch_review_keywords(self, session, review_id: UUID) -> List[str]:
        """리뷰의 aspect 키워드 목록."""
        rows = session.execute(
            text("SELECT keyword FROM review_aspects WHERE review_id = :rid"),
            {"rid": str(review_id)},
        ).fetchall()
        return [row[0] for row in rows if row[0]]

    def retrieve(self, session, review_id: UUID) -> List[OrgCandidate]:
        """키워드 교집합 점수 상위 top_k 후보를 반환한다.

        교집합이 하나도 없으면 조직 전량을 폴백 후보로 내려보낸다. 후보가 비면
        고를 대상 자체가 사라지고, 최상위 본부로만 좁히면 교집합 0인 리뷰
        (621건 중 533건)가 구조적으로 본부보다 아래로 배정될 수 없다.
        """
        orgs = self.load_organizations(session)
        if not orgs:
            return []

        keywords = self.fetch_review_keywords(session, review_id)
        keyword_set = {k for k in keywords if k}

        scored: List[OrgCandidate] = []
        for org in orgs:
            matched = sorted(keyword_set.intersection(org.keywords))
            if matched:
                scored.append(
                    OrgCandidate(
                        org_id=org.org_id,
                        org_name=org.org_name,
                        role_responsibility=org.role_responsibility,
                        keywords=org.keywords,
                        matched=matched,
                    )
                )

        if not scored:
            # 키워드 신호가 없을 땐 순위를 매길 근거도 없으므로 top_k 로 자르지
            # 않는다. 조직도가 지금(114행)보다 훨씬 커지면 이 경로는 의미 기반
            # 검색으로 바꿔야 한다.
            return list(orgs)

        # 점수 내림차순, 동점이면 상위 계층 우선. "확신 없으면 상위 조직" 정책과
        # 같은 방향이다.
        scored.sort(key=lambda c: (-c.score, org_depth(c.org_id), c.org_id))
        return scored[: self.top_k]


# ----------------------------------------------------------------------
# 저장
# ----------------------------------------------------------------------

_UPSERT_SQL = text(
    """
    INSERT INTO reviews_assigned
        (review_id, assigner, assigned_dept, assignment_reason,
         confidence, is_failed, try_number)
    VALUES
        (:review_id, :assigner, :assigned_dept, :assignment_reason,
         :confidence, :is_failed, :try_number)
    ON CONFLICT (review_id, assigner) DO UPDATE SET
        assigned_dept     = EXCLUDED.assigned_dept,
        assignment_reason = EXCLUDED.assignment_reason,
        confidence        = EXCLUDED.confidence,
        is_failed         = EXCLUDED.is_failed,
        try_number        = reviews_assigned.try_number + 1,
        updated_at        = NOW()
    """
)


def save_assignment(session, assignment: Assignment) -> None:
    """배정 결과를 UPSERT 한다. 실패한 배정도 is_failed 로 기록한다."""
    session.execute(
        _UPSERT_SQL,
        {
            "review_id": str(assignment.review_id),
            "assigner": assignment.assigner,
            "assigned_dept": assignment.assigned_dept,
            "assignment_reason": assignment.assignment_reason,
            "confidence": assignment.confidence,
            "is_failed": assignment.is_failed,
            "try_number": assignment.try_number,
        },
    )


def unclassified(
    review_id: UUID, reason: str, *, failed: bool = False, assigner: str = "rule"
) -> Assignment:
    """미분류 결과를 만든다. 연동 스펙상 confidence 는 0.0 이다."""
    return Assignment(
        review_id=review_id,
        assigned_dept=[UNCLASSIFIED],
        assignment_reason=reason,
        confidence=0.0,
        is_failed=failed,
        assigner=assigner,
    )


# ----------------------------------------------------------------------
# 규칙 기반 배정 (베이스라인)
# ----------------------------------------------------------------------


class _BatchAssignerBase:
    """배치 실행부. 두 배정기가 공유하며 ``assign()`` 과 ``ASSIGNER`` 만 다르다."""

    ASSIGNER = "base"

    def __init__(self, config_path: str = DEFAULT_CONFIG_PATH, top_k: int = DEFAULT_TOP_K):
        self.logger = get_logger(__name__)
        self.db_connector = DatabaseConnector(config_path)
        self.retriever = OrgCandidateRetriever(top_k=top_k)

    def assign(self, session, review_id: UUID) -> Assignment:  # pragma: no cover - 추상
        raise NotImplementedError

    def _fetch_target_ids(
        self,
        session,
        limit: Optional[int],
        target_date: Optional[date] = None,
        reassign: bool = False,
    ) -> List[UUID]:
        """배정 대상 review_id 를 고른다.

        이미 배정된 건은 기본적으로 제외한다. DAG 의 다른 Gold 스텝은 모두
        ``target_date`` 로 파티션되므로(dags/financial_review_pipeline.py), 날짜
        범위 없이 ANALYZED 전량을 훑으면 매일 과거 전량이 재처리된다. LLM
        배정기에서는 그것이 과거 리뷰 1건당 매일 유료 호출 1회를 뜻한다.

        제외 기준은 결과 테이블에 대한 NOT EXISTS 이며, 같은 방식이
        ``src.gold.action_analyzer._fetch_pending_ids`` 에 이미 쓰이고 있다.
        """
        clauses = ["rmi.processing_status = 'ANALYZED'"]
        params: dict = {}

        if not reassign:
            clauses.append(
                "NOT EXISTS (SELECT 1 FROM reviews_assigned ra "
                "WHERE ra.review_id = rmi.review_id AND ra.assigner = :assigner)"
            )
            params["assigner"] = self.ASSIGNER

        if target_date is not None:
            clauses.append("DATE_TRUNC('day', rmi.review_created_at)::date = :target_date")
            params["target_date"] = target_date

        sql = (
            "SELECT rmi.review_id FROM review_master_index rmi WHERE "
            + " AND ".join(clauses)
            + " ORDER BY rmi.review_created_at NULLS LAST, rmi.review_id"
        )
        if limit is not None:
            sql += " LIMIT :limit"
            params["limit"] = limit

        return [row[0] for row in session.execute(text(sql), params).fetchall()]

    def assign_batch(
        self,
        batch_size: int = 100,
        limit: Optional[int] = None,
        target_date: Optional[date] = None,
        reassign: bool = False,
    ) -> dict:
        """ANALYZED 리뷰를 배치로 배정한다.

        Args:
            target_date: 지정하면 그 날짜의 리뷰만 처리한다 (DAG 규약).
            reassign: True 면 이미 배정된 건도 다시 처리한다.

        Returns:
            {"total": int, "assigned": int, "unclassified": int, "failed": int}
        """
        session = self.db_connector.get_session()
        try:
            review_ids = self._fetch_target_ids(session, limit, target_date, reassign)
            if not review_ids:
                self.logger.info("부서 배정: 대상 리뷰 없음")
                return {"total": 0, "assigned": 0, "unclassified": 0, "failed": 0}

            self.logger.info(f"부서 배정 시작: {len(review_ids)}건")
            assigned = unclassified_count = failed = 0

            for i in range(0, len(review_ids), batch_size):
                for review_id in review_ids[i : i + batch_size]:
                    # 리뷰 단위 SAVEPOINT. assign() 이 하는 일은 사실상 DB 질의뿐이라
                    # 현실적인 실패 모드가 DB 오류인데, 세션이 실패 상태가 되면 뒤이은
                    # save_assignment 도 같이 죽는다. 중첩 트랜잭션으로 격리해야
                    # is_failed 행이 실제로 기록되고 남은 리뷰가 계속 처리된다.
                    try:
                        with session.begin_nested():
                            result = self.assign(session, review_id)
                            save_assignment(session, result)
                    except Exception as exc:  # noqa: BLE001
                        self.logger.warning(f"[{review_id}] 배정 실패: {exc}")
                        result = unclassified(
                            review_id,
                            f"{type(exc).__name__}: {exc}",
                            failed=True,
                            assigner=self.ASSIGNER,
                        )
                        with session.begin_nested():
                            save_assignment(session, result)

                    if result.is_failed:
                        failed += 1
                    elif result.assigned_dept == [UNCLASSIFIED]:
                        unclassified_count += 1
                    else:
                        assigned += 1

                session.commit()
                self.logger.info(
                    f"진행 중: {min(i + batch_size, len(review_ids))}/{len(review_ids)} "
                    f"(assigned={assigned}, unclassified={unclassified_count}, failed={failed})"
                )

            self.logger.info(
                f"부서 배정 완료: total={len(review_ids)}, assigned={assigned}, "
                f"unclassified={unclassified_count}, failed={failed}"
            )
            return {
                "total": len(review_ids),
                "assigned": assigned,
                "unclassified": unclassified_count,
                "failed": failed,
            }
        except Exception:
            session.rollback()
            self.logger.exception("부서 배정 배치 처리 중 예외 발생")
            raise
        finally:
            session.close()


class RuleBasedAssigner(_BatchAssignerBase):
    """키워드 교집합 개수로 배정하는 베이스라인.

    이슈 #40 의 원 스펙이다. LLM 배정과 같은 후보 집합을 쓰므로, 두 결과의
    차이는 판정 방식에서만 나온다.
    """

    ASSIGNER = "rule"

    def assign(self, session, review_id: UUID) -> Assignment:
        candidates = self.retriever.retrieve(session, review_id)
        if not candidates:
            return unclassified(
                review_id,
                "후보 부서 없음 (organizations 미적재 또는 조회 실패)",
                assigner=self.ASSIGNER,
            )

        best = candidates[0]
        if not best.matched:
            # 폴백 후보만 있는 경우. 규칙만으로는 고를 근거가 없다.
            return unclassified(review_id, "키워드 교집합 없음", assigner=self.ASSIGNER)

        # 분자(matched)는 중복 제거된 집합의 교집합이므로 분모도 같은 의미여야
        # 한다. 원본 리스트 길이를 쓰면 같은 키워드가 여러 aspect 에 걸린 리뷰의
        # 확신도만 낮아진다.
        matched_count = best.score
        total_keywords = len(set(self.retriever.fetch_review_keywords(session, review_id))) or 1
        confidence = matched_count / total_keywords

        return Assignment(
            review_id=review_id,
            assigner=self.ASSIGNER,
            assigned_dept=expand_org_path(best.org_id),
            assignment_reason=(
                f"키워드 교집합 {matched_count}건: {', '.join(best.matched)} "
                f"→ {best.org_name}({best.org_id})"
            ),
            confidence=round(confidence, 4),
        )


# ----------------------------------------------------------------------
# LLM 배정
# ----------------------------------------------------------------------


class DeptChoice(BaseModel):
    """LLM 이 채워야 하는 구조. Structured Outputs 는 전 필드를 필수로 요구한다."""

    org_id: str
    reason: str
    confidence: float


_LLM_INSTRUCTIONS = """당신은 금융 앱 리뷰를 담당 부서로 배정하는 분류기다.

규칙:
1. 아래 후보 부서 중 정확히 하나의 org_id 를 고른다. 후보에 없는 org_id 를 지어내지 마라.
2. 최하위 조직까지 특정할 근거가 부족하면 확신할 수 있는 상위 조직을 고른다.
   틀린 말단 부서보다 맞는 상위 부서가 낫다.
3. 어느 후보와도 맞지 않으면 org_id 에 "{unclassified}" 를 넣는다.
4. reason 은 리뷰의 어떤 표현이 그 부서의 업무와 닿는지 한국어 한 문장으로 쓴다.
5. confidence 는 이 배정이 맞을 확률에 대한 스스로의 추정이다 (0.0~1.0).
"""

_REVIEW_CONTEXT_SQL = text(
    """
    SELECT p.refined_text, a.rating
    FROM reviews_preprocessed p
    LEFT JOIN app_reviews a ON a.review_id = p.app_review_id
    WHERE p.review_id = :rid
    """
)

_ASPECTS_SQL = text(
    "SELECT keyword, sentiment_score, category FROM review_aspects WHERE review_id = :rid"
)


class LLMAssigner(_BatchAssignerBase):
    """후보를 좁힌 뒤 LLM 이 하나를 고르는 배정기.

    규칙 배정기와 **같은 후보 집합**을 받으므로 두 결과의 차이는 판정 방식에서만
    나온다. 후보를 좁히지 않고 114개 부서를 통째로 넣지 않는 이유도 같다 —
    입력이 달라지면 비교가 성립하지 않는다.

    temperature 는 0.0 이다. 같은 리뷰에 매번 다른 부서가 나오면 평가 수치가
    무엇을 재는지 알 수 없고, 재실행 시 배정이 흔들려 운영에서도 곤란하다.

    호출 1건당 리뷰 1건이다. 여러 리뷰를 한 번에 묶으면 토큰은 아끼지만 후보
    집합이 섞여 판정 근거가 흐려지고, 한 건이 깨질 때 배치 전체가 날아간다.
    """

    ASSIGNER = "llm"
    MODEL = os.getenv("DEPT_ASSIGN_MODEL", "gpt-4o-mini")
    TEMPERATURE = 0.0
    MAX_RETRIES = 3
    RETRY_BACKOFF_SECONDS = 2.0
    SOURCE_TABLE = "reviews_assigned"
    REVIEW_TEXT_LIMIT = 1000

    def __init__(self, config_path: str = DEFAULT_CONFIG_PATH, top_k: int = DEFAULT_TOP_K):
        super().__init__(config_path=config_path, top_k=top_k)
        self._client = self._init_client()

    def _init_client(self):
        if not OPENAI_AVAILABLE:
            self.logger.error("openai 패키지 없음 — LLM 배정 불가")
            return None
        api_key = os.getenv("OPENAI_API_KEY")
        if not api_key:
            self.logger.error("OPENAI_API_KEY 미설정 — LLM 배정 불가")
            return None
        try:
            return OpenAI(api_key=api_key, base_url=os.getenv("OPENAI_BASE_URL"), timeout=60.0)
        except Exception as exc:  # noqa: BLE001
            self.logger.error(f"OpenAI 클라이언트 초기화 실패: {exc}")
            return None

    # ------------------------------------------------------------------
    # 프롬프트
    # ------------------------------------------------------------------

    def fetch_review_context(self, session, review_id: UUID) -> dict:
        """리뷰 본문·별점·aspect 를 모은다."""
        row = session.execute(_REVIEW_CONTEXT_SQL, {"rid": str(review_id)}).fetchone()
        aspects = session.execute(_ASPECTS_SQL, {"rid": str(review_id)}).fetchall()
        return {
            "text": (row[0] if row else None) or "",
            "rating": row[1] if row else None,
            "aspects": [
                {"keyword": a[0], "sentiment": a[1], "category": a[2]} for a in aspects
            ],
        }

    def build_prompt(self, context: dict, candidates: Sequence[OrgCandidate]) -> str:
        lines = [f"리뷰 본문: {context['text'][: self.REVIEW_TEXT_LIMIT]}"]
        if context.get("rating") is not None:
            lines.append(f"별점: {context['rating']}")

        if context.get("aspects"):
            lines.append("\n추출된 속성:")
            for aspect in context["aspects"]:
                sentiment = aspect.get("sentiment")
                sentiment_text = f"{sentiment:.2f}" if isinstance(sentiment, float) else "-"
                lines.append(
                    f"- {aspect.get('keyword')} (감성 {sentiment_text}, "
                    f"분류 {aspect.get('category') or '-'})"
                )

        lines.append("\n후보 부서:")
        for candidate in candidates:
            role = (candidate.role_responsibility or "").replace("\n", " ").strip()
            matched = f" [키워드 일치: {', '.join(candidate.matched)}]" if candidate.matched else ""
            lines.append(f"- {candidate.org_id} | {candidate.org_name} | {role}{matched}")

        return "\n".join(lines)

    # ------------------------------------------------------------------
    # 배정
    # ------------------------------------------------------------------

    def assign(self, session, review_id: UUID) -> Assignment:
        if self._client is None:
            return unclassified(review_id, "LLM 클라이언트 없음", failed=True, assigner=self.ASSIGNER)

        candidates = self.retriever.retrieve(session, review_id)
        if not candidates:
            return unclassified(
                review_id,
                "후보 부서 없음 (organizations 미적재 또는 조회 실패)",
                assigner=self.ASSIGNER,
            )

        context = self.fetch_review_context(session, review_id)
        if not context["text"]:
            return unclassified(
                review_id,
                "전처리 본문 없음 (reviews_preprocessed 확인 필요)",
                assigner=self.ASSIGNER,
            )

        prompt = self.build_prompt(context, candidates)
        log = self._start_log(session, review_id, len(candidates))

        choice, usage, error = self._call_llm(prompt, review_id)
        if choice is None:
            self._finish_log(log, AnalysisStatusType.FAILED, usage=usage, error=error)
            return unclassified(
                review_id, error or "LLM 호출 실패", failed=True, assigner=self.ASSIGNER
            )

        allowed = {c.org_id for c in candidates}
        if choice.org_id != UNCLASSIFIED and choice.org_id not in allowed:
            # 후보 밖 org_id 는 환각이다. temperature 0 에서 재시도해도 같은 값이
            # 나오므로 실패로 표시해 재시도를 유도하지 않고 미분류로 닫는다.
            reason = f"LLM 이 후보 밖 org_id 반환: {choice.org_id}"
            self.logger.warning(f"[{review_id}] {reason}")
            self._finish_log(log, AnalysisStatusType.SUCCESS, usage=usage, payload=choice.model_dump())
            return unclassified(review_id, reason, assigner=self.ASSIGNER)

        self._finish_log(log, AnalysisStatusType.SUCCESS, usage=usage, payload=choice.model_dump())

        if choice.org_id == UNCLASSIFIED:
            return unclassified(
                review_id,
                choice.reason or "LLM 판정: 해당 부서 없음",
                assigner=self.ASSIGNER,
            )

        return Assignment(
            review_id=review_id,
            assigner=self.ASSIGNER,
            assigned_dept=expand_org_path(choice.org_id),
            assignment_reason=choice.reason,
            confidence=round(min(max(choice.confidence, 0.0), 1.0), 4),
        )

    def _call_llm(self, prompt: str, review_id: UUID):
        """(choice, usage, error) 를 돌려준다. 재시도는 여기서만 한다."""
        import time

        last_error: Optional[str] = None
        usage: Optional[dict] = None

        for attempt in range(1, self.MAX_RETRIES + 1):
            try:
                response = self._client.responses.parse(
                    model=self.MODEL,
                    instructions=_LLM_INSTRUCTIONS.format(unclassified=UNCLASSIFIED),
                    input=prompt,
                    temperature=self.TEMPERATURE,
                    text_format=DeptChoice,
                )
                usage = _extract_usage(response)

                refusal = _first_refusal(response)
                if refusal:
                    return None, usage, f"LLM 거부: {refusal}"

                parsed = getattr(response, "output_parsed", None)
                if parsed is None:
                    last_error = "구조화 응답 파싱 실패 (output_parsed 없음)"
                else:
                    return parsed, usage, None
            except RateLimitError as exc:
                last_error = f"RateLimitError: {exc}"
                self.logger.warning(f"[{review_id}] LLM RateLimit (시도 {attempt})")
            except APIError as exc:
                last_error = f"APIError: {exc}"
                self.logger.error(f"[{review_id}] LLM APIError (시도 {attempt}): {exc}")
                status_code = getattr(exc, "status_code", None)
                if status_code and status_code < 500:
                    break
            except Exception as exc:  # noqa: BLE001
                last_error = f"{type(exc).__name__}: {exc}"
                self.logger.error(f"[{review_id}] LLM 오류 (시도 {attempt}): {exc}")
                break

            if attempt < self.MAX_RETRIES:
                time.sleep(self.RETRY_BACKOFF_SECONDS * attempt)

        return None, usage, last_error or "max retries exceeded"

    # ------------------------------------------------------------------
    # 호출 로그 (토큰 사용량 포함)
    # ------------------------------------------------------------------

    def _start_log(self, session, review_id: UUID, candidate_count: int) -> LLMAnalysisLog:
        log = LLMAnalysisLog(
            source_table=self.SOURCE_TABLE,
            source_record_id=str(review_id),
            model_name=self.MODEL,
            params=json.dumps(
                {"temperature": self.TEMPERATURE, "top_k": self.retriever.top_k,
                 "candidates": candidate_count},
                ensure_ascii=False,
            ),
            status=AnalysisStatusType.PROCESSING,
        )
        session.add(log)
        session.flush()
        return log

    def _finish_log(
        self,
        log: LLMAnalysisLog,
        status: AnalysisStatusType,
        *,
        usage: Optional[dict] = None,
        payload: Optional[dict] = None,
        error: Optional[str] = None,
    ) -> None:
        """토큰 사용량을 남긴다. 상한을 두지 않기로 했으므로 사후 집계가 유일한 통제 수단이다."""
        log.status = status
        log.result_payload = {"choice": payload, "usage": usage}
        log.error_message = error
        log.processed_at = datetime.now(timezone.utc)


def _extract_usage(response) -> Optional[dict]:
    usage = getattr(response, "usage", None)
    if usage is None:
        return None
    return {
        "input_tokens": getattr(usage, "input_tokens", None),
        "output_tokens": getattr(usage, "output_tokens", None),
        "total_tokens": getattr(usage, "total_tokens", None),
    }


def _first_refusal(response) -> Optional[str]:
    """Structured Outputs 는 안전 거부를 refusal 콘텐츠로 돌려준다."""
    for item in getattr(response, "output", None) or []:
        for content in getattr(item, "content", None) or []:
            if getattr(content, "type", None) == "refusal":
                return getattr(content, "refusal", "") or "refusal"
    return None


def fetch_assignments(
    session,
    review_ids: Optional[Sequence[UUID]] = None,
    assigner: Optional[str] = None,
) -> Dict[str, Assignment]:
    """저장된 배정 결과를 review_id 문자열 키로 조회한다 (평가용).

    ``review_ids`` 가 None 이면 전량, 빈 시퀀스면 빈 결과다. 둘을 같이 취급하면
    "이 목록의 배정을 다오"가 조용히 전체 스캔으로 넓어진다.

    ``assigner`` 를 주면 그 배정기의 행만 본다. 같은 리뷰에 배정기별 1행이
    존재하므로, 평가에서 두 결과를 섞지 않으려면 필요하다.
    """
    if review_ids is not None and len(review_ids) == 0:
        return {}

    sql = (
        "SELECT review_id, assigned_dept, assignment_reason, confidence, "
        "is_failed, try_number, assigner FROM reviews_assigned"
    )
    clauses: List[str] = []
    params: dict = {}
    if review_ids is not None:
        # 바인딩된 리스트는 text[] 로 넘어가므로 uuid[] 로 캐스팅해야 비교가 성립한다.
        clauses.append("review_id = ANY(CAST(:ids AS uuid[]))")
        params["ids"] = [str(r) for r in review_ids]
    if assigner is not None:
        clauses.append("assigner = :assigner")
        params["assigner"] = assigner
    if clauses:
        sql += " WHERE " + " AND ".join(clauses)

    rows = session.execute(text(sql), params).fetchall()
    return {
        str(row[0]): Assignment(
            review_id=row[0],
            assigned_dept=list(row[1] or []),
            assignment_reason=row[2] or "",
            confidence=row[3] if row[3] is not None else 0.0,
            is_failed=bool(row[4]),
            try_number=row[5] or 1,
            assigner=row[6],
        )
        for row in rows
    }
