"""Tests for department assignment and its evaluation (Issue #40)

Coverage:
- 계층 경로 유틸 (expand_org_path / org_depth)
- OrgCandidateRetriever: 교집합 점수, 동점 시 상위 우선, top_k, 폴백
- RuleBasedAssigner: 배정/미분류/후보 없음, confidence 계산
- dept_eval: 라벨 로드, 계층 판정, 집계
- reviews_assigned UPSERT 멱등성 (실제 PostgreSQL + 리비전 20260813_0002)
"""

import importlib.util
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from sqlalchemy import text
from uuid6 import uuid7

from src.gold.dept_assigner import (
    DEFAULT_TOP_K,
    UNCLASSIFIED,
    Assignment,
    DeptChoice,
    LLMAssigner,
    OrgCandidate,
    OrgCandidateRetriever,
    RuleBasedAssigner,
    expand_org_path,
    fetch_assignments,
    org_depth,
    save_assignment,
    unclassified,
)
from src.gold.dept_eval import (
    Label,
    evaluate,
    format_report,
    load_labels,
    match_kind,
)
from src.models.apps import App
from src.models.enums import PlatformType, ProcessingStatusType
from src.models.review_master_index import ReviewMasterIndex

ROOT = Path(__file__).resolve().parents[1]
REVISION_PATH = ROOT / "alembic" / "versions" / "20260813_0002_reviews_assigned_review_id_unique.py"


# ─────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────

class _FakeResult:
    def __init__(self, rows):
        self._rows = rows

    def fetchall(self):
        return self._rows

    def fetchone(self):
        return self._rows[0] if self._rows else None


class _FakeSession:
    """organizations / review_aspects 두 질의만 답하는 세션 대역.

    retrieve() 가 어떤 SQL 을 언제 던지는지까지 검증 대상이므로, 질의를
    문자열로 구분해 답하고 호출 이력을 남긴다.
    """

    def __init__(self, orgs, keywords_by_review):
        self.orgs = orgs
        self.keywords_by_review = keywords_by_review
        self.queries = []

    def execute(self, statement, params=None):
        sql = str(statement)
        self.queries.append(sql)
        if "FROM organizations" in sql:
            return _FakeResult(self.orgs)
        if "FROM review_aspects" in sql:
            keywords = self.keywords_by_review.get((params or {})["rid"], [])
            return _FakeResult([(kw,) for kw in keywords])
        raise AssertionError(f"예상하지 못한 질의: {sql}")


def _org(org_id, name, keywords):
    """organizations 한 행 (org_id, org_name, role_responsibility, keywords)."""
    return (org_id, name, f"{name} 담당", keywords)


def _make_retriever(orgs, keywords_by_review, top_k=DEFAULT_TOP_K):
    retriever = OrgCandidateRetriever(top_k=top_k)
    return retriever, _FakeSession(orgs, keywords_by_review)


def _make_rule_assigner(orgs, keywords_by_review, top_k=DEFAULT_TOP_K):
    """DB 커넥터 없이 RuleBasedAssigner 를 만든다."""
    assigner = RuleBasedAssigner.__new__(RuleBasedAssigner)
    assigner.logger = MagicMock()
    assigner.db_connector = MagicMock()
    assigner.retriever = OrgCandidateRetriever(top_k=top_k)
    return assigner, _FakeSession(orgs, keywords_by_review)


def _load_revision_module():
    spec = importlib.util.spec_from_file_location("_rev_20260813_0002", REVISION_PATH)
    assert spec is not None and spec.loader is not None, f"리비전 모듈을 못 읽음: {REVISION_PATH}"
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


# ─────────────────────────────────────────────
# A. 계층 경로 유틸
# ─────────────────────────────────────────────

class TestOrgPath:
    def test_top_level_expands_to_itself(self):
        assert expand_org_path("1") == ["1"]

    def test_nested_expands_with_ancestors(self):
        assert expand_org_path("1-1-2") == ["1", "1-1", "1-1-2"]

    def test_two_digit_top_level_is_not_split_by_digit(self):
        assert expand_org_path("10-5") == ["10", "10-5"]

    @pytest.mark.parametrize("org_id,expected", [("1", 1), ("1-1", 2), ("10-5-3", 3)])
    def test_depth(self, org_id, expected):
        assert org_depth(org_id) == expected


# ─────────────────────────────────────────────
# B. 후보 검색
# ─────────────────────────────────────────────

class TestOrgCandidateRetriever:
    ORGS = [
        _org("1", "디지털본부", ["디지털", "모바일"]),
        _org("1-1", "채널부", ["모바일", "앱"]),
        _org("1-1-2", "앱운영팀", ["앱", "로그인", "인증"]),
        _org("2", "여신본부", ["대출", "심사"]),
    ]

    def test_scores_by_intersection_size(self):
        rid = uuid7()
        retriever, session = _make_retriever(self.ORGS, {str(rid): ["앱", "로그인", "인증"]})

        candidates = retriever.retrieve(session, rid)

        assert candidates[0].org_id == "1-1-2"
        assert candidates[0].matched == ["로그인", "앱", "인증"]
        assert candidates[0].score == 3

    def test_tie_prefers_shallower_org(self):
        rid = uuid7()
        # '모바일' 하나만 겹치므로 1(depth 1)과 1-1(depth 2)이 동점이다.
        retriever, session = _make_retriever(self.ORGS, {str(rid): ["모바일"]})

        candidates = retriever.retrieve(session, rid)

        assert [c.org_id for c in candidates] == ["1", "1-1"]

    def test_orgs_without_intersection_are_excluded(self):
        rid = uuid7()
        retriever, session = _make_retriever(self.ORGS, {str(rid): ["앱"]})

        candidates = retriever.retrieve(session, rid)

        assert "2" not in [c.org_id for c in candidates]

    def test_top_k_limits_candidates(self):
        rid = uuid7()
        retriever, session = _make_retriever(self.ORGS, {str(rid): ["모바일", "앱"]}, top_k=1)

        assert len(retriever.retrieve(session, rid)) == 1

    def test_falls_back_to_every_org_when_no_intersection(self):
        rid = uuid7()
        retriever, session = _make_retriever(self.ORGS, {str(rid): ["환율"]})

        candidates = retriever.retrieve(session, rid)

        # 최상위만 남기면 LLM 이 본부 아래로 내려갈 방법이 사라진다.
        assert [c.org_id for c in candidates] == ["1", "1-1", "1-1-2", "2"]
        assert all(c.matched == [] for c in candidates)

    def test_fallback_ignores_top_k(self):
        rid = uuid7()
        retriever, session = _make_retriever(self.ORGS, {str(rid): ["환율"]}, top_k=1)

        # 순위를 매길 신호가 없으므로 자르지 않는다.
        assert len(retriever.retrieve(session, rid)) == len(self.ORGS)

    def test_review_without_keywords_falls_back(self):
        rid = uuid7()
        retriever, session = _make_retriever(self.ORGS, {})

        assert [c.org_id for c in retriever.retrieve(session, rid)] == ["1", "1-1", "1-1-2", "2"]

    def test_empty_organizations_returns_no_candidates(self):
        rid = uuid7()
        retriever, session = _make_retriever([], {str(rid): ["앱"]})

        assert retriever.retrieve(session, rid) == []

    def test_organizations_are_loaded_once_per_retriever(self):
        retriever, session = _make_retriever(self.ORGS, {})
        rid_a, rid_b = uuid7(), uuid7()

        retriever.retrieve(session, rid_a)
        retriever.retrieve(session, rid_b)

        org_queries = [q for q in session.queries if "FROM organizations" in q]
        assert len(org_queries) == 1


class TestOrgCandidateScore:
    def test_score_is_matched_count(self):
        candidate = OrgCandidate(org_id="1", org_name="본부", role_responsibility=None)
        assert candidate.score == 0

        candidate.matched = ["앱", "로그인"]
        assert candidate.score == 2


# ─────────────────────────────────────────────
# C. 규칙 기반 배정
# ─────────────────────────────────────────────

class TestRuleBasedAssigner:
    ORGS = TestOrgCandidateRetriever.ORGS

    def test_assigns_full_ancestor_path(self):
        rid = uuid7()
        assigner, session = _make_rule_assigner(self.ORGS, {str(rid): ["앱", "로그인", "인증"]})

        result = assigner.assign(session, rid)

        assert result.assigned_dept == ["1", "1-1", "1-1-2"]
        assert result.is_failed is False
        assert "앱운영팀(1-1-2)" in result.assignment_reason

    def test_confidence_is_matched_over_total_keywords(self):
        rid = uuid7()
        assigner, session = _make_rule_assigner(self.ORGS, {str(rid): ["앱", "로그인", "환율", "수수료"]})

        result = assigner.assign(session, rid)

        # 4개 중 2개('앱','로그인')가 1-1-2 와 겹친다.
        assert result.confidence == pytest.approx(0.5)

    def test_confidence_never_exceeds_one(self):
        rid = uuid7()
        # 중복 키워드가 있으면 교집합(집합)보다 총 키워드 수가 커지므로 1.0 을 넘지 않는다.
        assigner, session = _make_rule_assigner(self.ORGS, {str(rid): ["앱", "앱", "로그인"]})

        result = assigner.assign(session, rid)

        assert 0.0 < result.confidence <= 1.0

    def test_no_intersection_is_unclassified(self):
        rid = uuid7()
        assigner, session = _make_rule_assigner(self.ORGS, {str(rid): ["환율"]})

        result = assigner.assign(session, rid)

        assert result.assigned_dept == [UNCLASSIFIED]
        assert result.confidence == 0.0
        assert result.is_failed is False
        assert result.assignment_reason == "키워드 교집합 없음"

    def test_missing_organizations_is_unclassified(self):
        rid = uuid7()
        assigner, session = _make_rule_assigner([], {str(rid): ["앱"]})

        result = assigner.assign(session, rid)

        assert result.assigned_dept == [UNCLASSIFIED]
        assert "후보 부서 없음" in result.assignment_reason


class TestUnclassifiedHelper:
    def test_defaults_are_not_failed(self):
        result = unclassified(uuid7(), "사유")
        assert result.assigned_dept == [UNCLASSIFIED]
        assert result.confidence == 0.0
        assert result.is_failed is False

    def test_failed_flag_is_preserved(self):
        result = unclassified(uuid7(), "사유", failed=True)
        assert result.is_failed is True


# ─────────────────────────────────────────────
# D. 평가
# ─────────────────────────────────────────────

class TestMatchKind:
    @pytest.mark.parametrize(
        "predicted,label,expected",
        [
            (["1", "1-1", "1-1-2"], "1-1-2", "exact"),
            (["1", "1-1"], "1-1-2", "ancestor"),
            (["1", "1-1", "1-1-2"], "1-1", "descendant"),
            (["1", "1-2"], "1-1-2", "top_level"),
            (["2", "2-1"], "1-1-2", "miss"),
            ([UNCLASSIFIED], "1-1-2", "unclassified"),
            (None, "1-1-2", "missing"),
            ([], "1-1-2", "missing"),
        ],
    )
    def test_hierarchy_relations(self, predicted, label, expected):
        assert match_kind(predicted, label) == expected

    def test_two_digit_top_level_is_not_prefix_matched_by_string(self):
        # '1' 과 '10' 은 문자열 prefix 지만 다른 본부다.
        assert match_kind(["10", "10-5"], "1-1") == "miss"


class TestLoadLabels:
    HEADER = "no,review_id,service,platform,rating,review_text,assigned_dept,stopped_at_parent,ambiguous,memo\n"

    def _write(self, tmp_path, body, *, bom=False):
        path = tmp_path / "labels.csv"
        content = self.HEADER + body
        path.write_text(content, encoding="utf-8-sig" if bom else "utf-8")
        return path

    def test_reads_labeled_rows(self, tmp_path):
        path = self._write(tmp_path, "1,rid-a,신한,PLAYSTORE,1,본문,1-1-2,,,\n")

        labels = load_labels(path)

        assert len(labels) == 1
        assert labels[0].review_id == "rid-a"
        assert labels[0].org_id == "1-1-2"

    def test_skips_unlabeled_rows(self, tmp_path):
        path = self._write(
            tmp_path,
            "1,rid-a,신한,PLAYSTORE,1,본문,1-1-2,,,\n2,rid-b,신한,PLAYSTORE,2,본문,,,,\n",
        )

        assert [label.review_id for label in load_labels(path)] == ["rid-a"]

    def test_handles_bom_from_excel(self, tmp_path):
        path = self._write(tmp_path, "1,rid-a,신한,PLAYSTORE,1,본문,1-1-2,,,\n", bom=True)

        assert load_labels(path)[0].review_id == "rid-a"

    def test_reads_boolean_flags(self, tmp_path):
        path = self._write(tmp_path, "1,rid-a,신한,PLAYSTORE,1,본문,1-1,Y,1,애매함\n")

        label = load_labels(path)[0]

        assert label.stopped_at_parent is True
        assert label.ambiguous is True
        assert label.memo == "애매함"

    def test_missing_label_column_raises(self, tmp_path):
        path = tmp_path / "bad.csv"
        path.write_text("no,review_id\n1,rid-a\n", encoding="utf-8")

        with pytest.raises(ValueError, match="assigned_dept"):
            load_labels(path)


class TestEvaluate:
    def _assignment(self, review_id, dept):
        return Assignment(
            review_id=review_id,
            assigned_dept=dept,
            assignment_reason="테스트",
            confidence=0.5,
        )

    def test_counts_each_kind(self):
        labels = [
            Label(review_id="a", org_id="1-1-2"),
            Label(review_id="b", org_id="1-1-2"),
            Label(review_id="c", org_id="1-1-2"),
            Label(review_id="d", org_id="1-1-2"),
        ]
        assignments = {
            "a": self._assignment("a", ["1", "1-1", "1-1-2"]),
            "b": self._assignment("b", ["1", "1-1"]),
            "c": self._assignment("c", [UNCLASSIFIED]),
            # d 는 배정 결과 없음
        }

        result = evaluate(labels, assignments)

        assert result.total == 4
        assert result.counts == {"exact": 1, "ancestor": 1, "unclassified": 1, "missing": 1}
        assert result.exact_rate == pytest.approx(0.25)
        assert result.hierarchy_rate == pytest.approx(0.5)
        assert result.unclassified_rate == pytest.approx(0.25)

    def test_ambiguous_labels_stay_in_denominator(self):
        labels = [
            Label(review_id="a", org_id="1-1", ambiguous=True),
            Label(review_id="b", org_id="1-1"),
        ]
        assignments = {"b": self._assignment("b", ["1", "1-1"])}

        result = evaluate(labels, assignments)

        assert result.total == 2
        assert result.ambiguous_ids == ["a"]
        assert result.exact_rate == pytest.approx(0.5)

    def test_empty_labels_do_not_divide_by_zero(self):
        result = evaluate([], {})

        assert result.total == 0
        assert result.exact_rate == 0.0
        assert result.hierarchy_rate == 0.0

    def test_report_shows_counts_and_rates(self):
        labels = [Label(review_id="a", org_id="1-1")]
        assignments = {"a": self._assignment("a", ["1", "1-1"])}

        report = format_report(evaluate(labels, assignments))

        assert "exact" in report
        assert "100.0%" in report


# ─────────────────────────────────────────────
# E. 저장 (실제 PostgreSQL)
# ─────────────────────────────────────────────

@pytest.fixture
def reviews_assigned_unique(test_db_session):
    """리비전 20260813_0002 를 테스트 스키마에 적용한다.

    conftest 는 sql/schema_v4.sql(= Alembic 베이스라인 스냅샷)로 스키마를 만들고,
    UNIQUE 제약은 그 다음 리비전이 건다. 여기서 DDL 을 다시 쓰면 마이그레이션과
    드리프트가 생기므로 리비전 모듈의 upgrade() 를 그대로 실행한다.
    """
    from alembic.operations import Operations
    from alembic.runtime.migration import MigrationContext

    connection = test_db_session.connection()
    migration_context = MigrationContext.configure(connection)
    revision = _load_revision_module()

    with Operations.context(migration_context):
        revision.upgrade()

    return test_db_session


@pytest.fixture
def assigned_review_id(reviews_assigned_unique):
    """reviews_assigned 의 부모 행을 만든다.

    reviews_assigned.review_id 에는 review_master_index 로 향하는 FK 가 있어
    (fk_review_master_index_to_reviews_assigned) 리뷰 없이는 배정을 넣을 수 없다.
    """
    now = datetime.now(timezone.utc)
    app = App(
        app_id=uuid7(),
        platform_app_id="dept-assign-test",
        platform_type=PlatformType.PLAYSTORE,
        name="Dept Assign Test App",
    )
    review = ReviewMasterIndex(
        review_id=uuid7(),
        app_id=app.app_id,
        platform_review_id="dept-assign-review-0",
        platform_type=PlatformType.PLAYSTORE,
        review_created_at=now,
        ingested_at=now,
        processing_status=ProcessingStatusType.ANALYZED,
        is_active=True,
        is_reply=False,
    )
    reviews_assigned_unique.add_all([app, review])
    reviews_assigned_unique.flush()
    return review.review_id


@pytest.mark.requires_db
@pytest.mark.integration
class TestSaveAssignment:
    def test_insert_then_read_back(self, reviews_assigned_unique, assigned_review_id):
        session = reviews_assigned_unique
        review_id = assigned_review_id

        save_assignment(
            session,
            Assignment(
                review_id=review_id,
                assigned_dept=["1", "1-1"],
                assignment_reason="키워드 교집합 1건",
                confidence=0.5,
            ),
        )
        session.flush()

        stored = fetch_assignments(session, [review_id])

        assert list(stored) == [str(review_id)]
        assert stored[str(review_id)].assigned_dept == ["1", "1-1"]
        assert stored[str(review_id)].try_number == 1

    def test_rerun_updates_in_place_and_bumps_try_number(
        self, reviews_assigned_unique, assigned_review_id
    ):
        session = reviews_assigned_unique
        review_id = assigned_review_id

        save_assignment(
            session,
            Assignment(
                review_id=review_id,
                assigned_dept=["1", "1-1"],
                assignment_reason="첫 배정",
                confidence=0.5,
            ),
        )
        session.flush()
        save_assignment(
            session,
            Assignment(
                review_id=review_id,
                assigned_dept=["2"],
                assignment_reason="재배정",
                confidence=0.9,
            ),
        )
        session.flush()

        rows = session.execute(
            text(
                "SELECT assigned_dept, assignment_reason, confidence, try_number "
                "FROM reviews_assigned WHERE review_id = :rid"
            ),
            {"rid": str(review_id)},
        ).fetchall()

        assert len(rows) == 1
        assert rows[0][0] == ["2"]
        assert rows[0][1] == "재배정"
        assert rows[0][2] == pytest.approx(0.9)
        assert rows[0][3] == 2

    def test_failed_assignment_is_recorded(self, reviews_assigned_unique, assigned_review_id):
        session = reviews_assigned_unique
        review_id = assigned_review_id

        save_assignment(session, unclassified(review_id, "RuntimeError: boom", failed=True))
        session.flush()

        stored = fetch_assignments(session, [review_id])[str(review_id)]

        assert stored.is_failed is True
        assert stored.assigned_dept == [UNCLASSIFIED]
        assert stored.confidence == 0.0

    def test_unique_constraint_exists(self, reviews_assigned_unique):
        session = reviews_assigned_unique

        constraint = session.execute(
            text(
                "SELECT conname FROM pg_constraint "
                "WHERE conrelid = 'reviews_assigned'::regclass AND contype = 'u' "
                "AND conname = 'uq_reviews_assigned_review_id'"
            )
        ).fetchone()

        assert constraint is not None


# ─────────────────────────────────────────────
# F. LLM 배정
# ─────────────────────────────────────────────

class _FakeUsage:
    input_tokens = 1200
    output_tokens = 40
    total_tokens = 1240


class _FakeRefusalContent:
    type = "refusal"
    refusal = "정책상 응답할 수 없습니다"


class _FakeOutputItem:
    def __init__(self, content):
        self.content = content


class _FakeResponse:
    def __init__(self, parsed=None, output=None):
        self.output_parsed = parsed
        self.output = output or []
        self.usage = _FakeUsage()


class _FakeResponses:
    def __init__(self, response):
        self._response = response
        self.calls = []

    def parse(self, **kwargs):
        self.calls.append(kwargs)
        if isinstance(self._response, Exception):
            raise self._response
        return self._response


class _FakeOpenAI:
    def __init__(self, response):
        self.responses = _FakeResponses(response)


def _make_llm_assigner(orgs, keywords_by_review, response=None):
    """실제 OpenAI 호출 없이 LLMAssigner 를 만든다.

    행복 경로는 실제 API 로 검증했다(PR 설명 참조). 여기서 대역을 쓰는 것은
    후보 밖 org_id·거부·예외처럼 실제 API 에 요청해서 만들어낼 수 없는
    분기를 재현하기 위해서다.
    """
    assigner = LLMAssigner.__new__(LLMAssigner)
    assigner.logger = MagicMock()
    assigner.db_connector = MagicMock()
    assigner.retriever = OrgCandidateRetriever(top_k=DEFAULT_TOP_K)
    assigner._client = _FakeOpenAI(response) if response is not None else None
    return assigner, _FakeSession(orgs, keywords_by_review)


class _LLMFakeSession(_FakeSession):
    """리뷰 본문 질의와 LLM 로그 add/flush 까지 받는 세션 대역."""

    def __init__(self, orgs, keywords_by_review, text_by_review=None, rating=3):
        super().__init__(orgs, keywords_by_review)
        self.text_by_review = text_by_review or {}
        self.rating = rating
        self.added = []

    def execute(self, statement, params=None):
        sql = str(statement)
        if "reviews_preprocessed" in sql:
            self.queries.append(sql)
            rid = (params or {})["rid"]
            return _FakeResult([(self.text_by_review.get(rid), self.rating)])
        if "sentiment_score" in sql:
            self.queries.append(sql)
            rid = (params or {})["rid"]
            return _FakeResult([(kw, 0.2, "APP") for kw in self.keywords_by_review.get(rid, [])])
        return super().execute(statement, params)

    def add(self, obj):
        self.added.append(obj)

    def flush(self):
        pass


def _make_llm_session(orgs, keywords, text_by_review):
    return _LLMFakeSession(orgs, keywords, text_by_review)


class TestLLMAssigner:
    ORGS = TestOrgCandidateRetriever.ORGS

    def _setup(self, response, keywords, review_text="로그인이 안 됩니다"):
        rid = uuid7()
        assigner, _ = _make_llm_assigner(self.ORGS, {}, response=response)
        session = _make_llm_session(self.ORGS, {str(rid): keywords}, {str(rid): review_text})
        return assigner, session, rid

    def test_assigns_choice_with_ancestor_path(self):
        response = _FakeResponse(parsed=DeptChoice(org_id="1-1-2", reason="로그인 장애", confidence=0.9))
        assigner, session, rid = self._setup(response, ["로그인"])

        result = assigner.assign(session, rid)

        assert result.assigned_dept == ["1", "1-1", "1-1-2"]
        assert result.confidence == pytest.approx(0.9)
        assert result.assignment_reason == "로그인 장애"
        assert result.is_failed is False

    def test_uses_zero_temperature(self):
        response = _FakeResponse(parsed=DeptChoice(org_id="1-1-2", reason="r", confidence=0.5))
        assigner, session, rid = self._setup(response, ["로그인"])

        assigner.assign(session, rid)

        assert assigner._client.responses.calls[0]["temperature"] == 0.0
        assert assigner._client.responses.calls[0]["text_format"] is DeptChoice

    def test_org_id_outside_candidates_is_rejected(self):
        response = _FakeResponse(parsed=DeptChoice(org_id="99-9", reason="지어냄", confidence=0.99))
        assigner, session, rid = self._setup(response, ["로그인"])

        result = assigner.assign(session, rid)

        assert result.assigned_dept == [UNCLASSIFIED]
        assert "99-9" in result.assignment_reason
        # temperature 0 에서 재시도해도 같은 값이 나오므로 실패로 표시하지 않는다.
        assert result.is_failed is False

    def test_model_may_decline_to_choose(self):
        response = _FakeResponse(parsed=DeptChoice(org_id=UNCLASSIFIED, reason="근거 부족", confidence=0.0))
        assigner, session, rid = self._setup(response, ["로그인"])

        result = assigner.assign(session, rid)

        assert result.assigned_dept == [UNCLASSIFIED]
        assert result.assignment_reason == "근거 부족"
        assert result.is_failed is False

    def test_confidence_is_clamped(self):
        response = _FakeResponse(parsed=DeptChoice(org_id="1-1-2", reason="r", confidence=1.7))
        assigner, session, rid = self._setup(response, ["로그인"])

        assert assigner.assign(session, rid).confidence == 1.0

    def test_refusal_is_a_failure(self):
        response = _FakeResponse(output=[_FakeOutputItem([_FakeRefusalContent()])])
        assigner, session, rid = self._setup(response, ["로그인"])

        result = assigner.assign(session, rid)

        assert result.is_failed is True
        assert "거부" in result.assignment_reason

    def test_missing_client_fails_without_calling(self):
        rid = uuid7()
        assigner, _ = _make_llm_assigner(self.ORGS, {})
        session = _make_llm_session(self.ORGS, {str(rid): ["로그인"]}, {str(rid): "본문"})

        result = assigner.assign(session, rid)

        assert result.is_failed is True
        assert result.assigned_dept == [UNCLASSIFIED]

    def test_missing_refined_text_is_unclassified(self):
        response = _FakeResponse(parsed=DeptChoice(org_id="1-1-2", reason="r", confidence=0.5))
        assigner, session, rid = self._setup(response, ["로그인"], review_text=None)

        result = assigner.assign(session, rid)

        assert result.assigned_dept == [UNCLASSIFIED]
        assert "본문 없음" in result.assignment_reason

    def test_usage_is_logged(self):
        response = _FakeResponse(parsed=DeptChoice(org_id="1-1-2", reason="r", confidence=0.5))
        assigner, session, rid = self._setup(response, ["로그인"])

        assigner.assign(session, rid)

        log = session.added[0]
        assert log.result_payload["usage"]["total_tokens"] == 1240
        assert log.result_payload["choice"]["org_id"] == "1-1-2"

    def test_prompt_lists_candidates_with_matches(self):
        assigner, _ = _make_llm_assigner(self.ORGS, {})
        candidates = [
            OrgCandidate(
                org_id="1-1-2",
                org_name="앱운영팀",
                role_responsibility="앱 운영",
                matched=["로그인"],
            )
        ]

        prompt = assigner.build_prompt(
            {"text": "로그인이 안 됨", "rating": 1, "aspects": [{"keyword": "로그인", "sentiment": 0.1, "category": "APP"}]},
            candidates,
        )

        assert "1-1-2 | 앱운영팀" in prompt
        assert "키워드 일치: 로그인" in prompt
        assert "별점: 1" in prompt
