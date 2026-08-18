"""Gold Layer - 부서 배정 (Department Assignment)

ANALYZED 상태 리뷰를 organizations 의 담당 부서에 배정하고 reviews_assigned 에
적재한다. 배정기는 두 구현이 같은 계약을 공유한다.

- ``RuleBasedAssigner``: review_aspects.keyword[] 와 organizations.keywords[] 의
  교집합 개수로 배정. 이슈 #40 원 스펙이며 LLM 배정의 평가 베이스라인이다.
- ``LLMAssigner``: 후보를 좁힌 뒤 LLM 이 판정 (별도 작업).

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

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Protocol, Sequence
from uuid import UUID

from sqlalchemy import text

from src.utils.db_connector import DatabaseConnector
from src.utils.logger import get_logger

UNCLASSIFIED = "미분류"

# 교집합이 하나도 없을 때 후보로 내려보낼 최상위 본부 수. organizations 는
# 13개 본부 아래 계층이 붙는 구조라, 폴백은 본부 레벨로만 제한한다.
_TOP_LEVEL_FALLBACK = 13

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

    def as_dict(self) -> dict:
        return {
            "review_id": str(self.review_id),
            "assigned_dept": self.assigned_dept,
            "assignment_reason": self.assignment_reason,
            "confidence": self.confidence,
            "is_failed": self.is_failed,
            "try_number": self.try_number,
        }


class Assigner(Protocol):
    """배정기 계약. 규칙/LLM 구현이 서로 교체 가능해야 평가가 성립한다."""

    def assign(self, session, review_id: UUID) -> Assignment: ...

    def assign_batch(self, batch_size: int = 100, limit: Optional[int] = None) -> dict: ...


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

        교집합이 하나도 없으면 최상위 본부를 폴백 후보로 내려보낸다. 후보가
        비면 LLM 이 고를 대상 자체가 사라지기 때문이다.
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
            top_level = [o for o in orgs if org_depth(o.org_id) == 1]
            return top_level[:_TOP_LEVEL_FALLBACK]

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
        (review_id, assigned_dept, assignment_reason, confidence, is_failed, try_number)
    VALUES
        (:review_id, :assigned_dept, :assignment_reason, :confidence, :is_failed, :try_number)
    ON CONFLICT (review_id) DO UPDATE SET
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
            "assigned_dept": assignment.assigned_dept,
            "assignment_reason": assignment.assignment_reason,
            "confidence": assignment.confidence,
            "is_failed": assignment.is_failed,
            "try_number": assignment.try_number,
        },
    )


def unclassified(review_id: UUID, reason: str, *, failed: bool = False) -> Assignment:
    """미분류 결과를 만든다. 연동 스펙상 confidence 는 0.0 이다."""
    return Assignment(
        review_id=review_id,
        assigned_dept=[UNCLASSIFIED],
        assignment_reason=reason,
        confidence=0.0,
        is_failed=failed,
    )


# ----------------------------------------------------------------------
# 규칙 기반 배정 (베이스라인)
# ----------------------------------------------------------------------


class RuleBasedAssigner:
    """키워드 교집합 개수로 배정하는 베이스라인.

    이슈 #40 의 원 스펙이다. LLM 배정과 같은 후보 집합을 쓰므로, 두 결과의
    차이는 판정 방식에서만 나온다.
    """

    def __init__(self, config_path: str = "config/crawler_config.yml", top_k: int = DEFAULT_TOP_K):
        self.logger = get_logger(__name__)
        self.db_connector = DatabaseConnector(config_path)
        self.retriever = OrgCandidateRetriever(top_k=top_k)

    def assign(self, session, review_id: UUID) -> Assignment:
        candidates = self.retriever.retrieve(session, review_id)
        if not candidates:
            return unclassified(review_id, "후보 부서 없음 (organizations 미적재 또는 조회 실패)")

        best = candidates[0]
        if not best.matched:
            # 폴백 후보만 있는 경우. 규칙만으로는 고를 근거가 없다.
            return unclassified(review_id, "키워드 교집합 없음")

        matched_count = best.score
        total_keywords = len(self.retriever.fetch_review_keywords(session, review_id)) or 1
        confidence = min(matched_count / total_keywords, 1.0)

        return Assignment(
            review_id=review_id,
            assigned_dept=expand_org_path(best.org_id),
            assignment_reason=(
                f"키워드 교집합 {matched_count}건: {', '.join(best.matched)} "
                f"→ {best.org_name}({best.org_id})"
            ),
            confidence=round(confidence, 4),
        )

    def _fetch_target_ids(self, session, limit: Optional[int]) -> List[UUID]:
        query = text(
            "SELECT review_id FROM review_master_index "
            "WHERE processing_status = 'ANALYZED' "
            "ORDER BY review_created_at NULLS LAST, review_id"
            + (" LIMIT :limit" if limit is not None else "")
        )
        params = {"limit": limit} if limit is not None else {}
        return [row[0] for row in session.execute(query, params).fetchall()]

    def assign_batch(self, batch_size: int = 100, limit: Optional[int] = None) -> dict:
        """ANALYZED 리뷰를 배치로 배정한다.

        Returns:
            {"total": int, "assigned": int, "unclassified": int, "failed": int}
        """
        session = self.db_connector.get_session()
        try:
            review_ids = self._fetch_target_ids(session, limit)
            if not review_ids:
                self.logger.info("부서 배정: 대상 리뷰 없음")
                return {"total": 0, "assigned": 0, "unclassified": 0, "failed": 0}

            self.logger.info(f"부서 배정 시작: {len(review_ids)}건")
            assigned = unclassified_count = failed = 0

            for i in range(0, len(review_ids), batch_size):
                for review_id in review_ids[i : i + batch_size]:
                    try:
                        result = self.assign(session, review_id)
                    except Exception as exc:  # noqa: BLE001
                        self.logger.warning(f"[{review_id}] 배정 실패: {exc}")
                        result = unclassified(review_id, f"{type(exc).__name__}: {exc}", failed=True)

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


def fetch_assignments(session, review_ids: Optional[Sequence[UUID]] = None) -> Dict[str, Assignment]:
    """저장된 배정 결과를 review_id 문자열 키로 조회한다 (평가용)."""
    sql = (
        "SELECT review_id, assigned_dept, assignment_reason, confidence, is_failed, try_number "
        "FROM reviews_assigned"
    )
    params: dict = {}
    if review_ids:
        # 바인딩된 리스트는 text[] 로 넘어가므로 uuid[] 로 캐스팅해야 비교가 성립한다.
        sql += " WHERE review_id = ANY(CAST(:ids AS uuid[]))"
        params["ids"] = [str(r) for r in review_ids]

    rows = session.execute(text(sql), params).fetchall()
    return {
        str(row[0]): Assignment(
            review_id=row[0],
            assigned_dept=list(row[1] or []),
            assignment_reason=row[2] or "",
            confidence=row[3] if row[3] is not None else 0.0,
            is_failed=bool(row[4]),
            try_number=row[5] or 1,
        )
        for row in rows
    }
