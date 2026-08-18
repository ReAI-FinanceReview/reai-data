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
from datetime import date
from typing import Dict, List, Optional, Protocol, Sequence
from uuid import UUID

from sqlalchemy import text

from src.utils.db_connector import DatabaseConnector
from src.utils.logger import get_logger

UNCLASSIFIED = "미분류"

# 배정기 식별자. 마이그레이션 20260813_0002 의 server_default 와 반드시 같아야
# 한다 — 어긋나면 리비전 이전에 적재된 행이 anti-join 에서 빠져 과거 전량이
# 조용히 재처리된다. tests 가 그 일치를 단언한다.
ASSIGNER_RULE = "rule"
ASSIGNER_LLM = "llm"

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
    # 어느 배정기가 만든 행인지. 기본값을 두지 않는다 — 빠뜨린 호출이 'rule' 행을
    # 만들면 ON CONFLICT (review_id, assigner) 가 규칙 베이스라인을 덮어쓴다.
    assigner: str = field(default=ASSIGNER_RULE, kw_only=True)

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
    review_id: UUID, reason: str, *, failed: bool = False, assigner: str = ASSIGNER_RULE
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


class RuleBasedAssigner:
    """키워드 교집합 개수로 배정하는 베이스라인.

    이슈 #40 의 원 스펙이다. LLM 배정과 같은 후보 집합을 쓰므로, 두 결과의
    차이는 판정 방식에서만 나온다.
    """

    ASSIGNER = ASSIGNER_RULE
    # 실패 행을 몇 번까지 다시 시도할지. 넘으면 기본 경로에서 제외된다.
    MAX_TRIES = 3

    def __init__(self, config_path: str = DEFAULT_CONFIG_PATH, top_k: int = DEFAULT_TOP_K):
        self.logger = get_logger(__name__)
        self.db_connector = DatabaseConnector(config_path)
        self.retriever = OrgCandidateRetriever(top_k=top_k)

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
            # 실패 행(is_failed)은 '배정됨'으로 세지 않는다. 세면 일시적 DB 오류
            # 한 번이 그 리뷰를 기본 경로에서 영구히 떨어뜨리고, 복구 수단이
            # 전량 재처리뿐이라 LLM 배정기에서는 과거 전량 재과금이 된다.
            # 무한 재시도는 try_number 상한으로 막는다.
            clauses.append(
                "NOT EXISTS (SELECT 1 FROM reviews_assigned ra "
                "WHERE ra.review_id = rmi.review_id AND ra.assigner = :assigner "
                "AND (NOT ra.is_failed OR ra.try_number >= :max_tries))"
            )
            params["assigner"] = self.ASSIGNER
            params["max_tries"] = self.MAX_TRIES

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


def fetch_assignments(
    session,
    review_ids: Optional[Sequence[UUID]] = None,
    assigner: Optional[str] = None,
) -> Dict[str, Assignment]:
    """저장된 배정 결과를 review_id 문자열 키로 조회한다 (평가용).

    ``review_ids`` 가 None 이면 전량, 빈 시퀀스면 빈 결과다. 둘을 같이 취급하면
    "이 목록의 배정을 다오"가 조용히 전체 스캔으로 넓어진다.

    ``assigner`` 를 주면 그 배정기의 행만 본다. 반환이 review_id 로 키잉된 dict
    이므로 배정기별 행을 동시에 담을 수 없다 — 필터 없이 호출하면 같은 리뷰의
    규칙/LLM 행 중 하나가 스캔 순서대로 버려진다. 그래서 필터를 생략한 호출이
    여러 배정기의 행을 만나면 예외를 던진다. 조용히 하나를 고르지 않는다.
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
    if assigner is None:
        seen = {}
        for row in rows:
            key = str(row[0])
            if key in seen and seen[key] != row[6]:
                raise ValueError(
                    f"리뷰 {key} 에 배정기가 둘 이상 있다({seen[key]}, {row[6]}). "
                    "assigner 를 지정해 어느 결과를 볼지 정하라."
                )
            seen[key] = row[6]

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
