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
    ASSIGNER_RULE,
    DEFAULT_TOP_K,
    UNCLASSIFIED,
    Assignment,
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
    # 동점 후보(1, 1-1)를 깊이 **역순**으로 배치한다. 얕은 것이 먼저 오면
    # 파이썬의 stable sort 만으로 기대 순서가 재현되어, 정렬 키의 깊이 tiebreak 를
    # 통째로 지워도 테스트가 통과한다(실제로 그런 상태였다).
    ORGS = [
        _org("1-1", "채널부", ["모바일", "앱"]),
        _org("1", "디지털본부", ["디지털", "모바일"]),
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

        # 최상위만 남기면 본부 아래로 내려갈 방법이 사라진다 (교집합 0 인 리뷰가 86%).
        assert [c.org_id for c in candidates] == ["1-1", "1", "1-1-2", "2"]
        assert all(c.matched == [] for c in candidates)

    def test_fallback_ignores_top_k(self):
        rid = uuid7()
        retriever, session = _make_retriever(self.ORGS, {str(rid): ["환율"]}, top_k=1)

        # 순위를 매길 신호가 없으므로 자르지 않는다.
        assert len(retriever.retrieve(session, rid)) == len(self.ORGS)

    def test_review_without_keywords_falls_back(self):
        rid = uuid7()
        retriever, session = _make_retriever(self.ORGS, {})

        assert [c.org_id for c in retriever.retrieve(session, rid)] == ["1-1", "1", "1-1-2", "2"]

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

    def test_duplicate_keywords_do_not_deflate_confidence(self):
        """분모도 중복 제거된 집합이라야 같은 매칭이 같은 확신도를 낸다."""
        rid_dup, rid_uniq = uuid7(), uuid7()
        dup, _ = _make_rule_assigner(self.ORGS, {str(rid_dup): ["앱", "앱", "로그인"]})
        dup_session = _FakeSession(self.ORGS, {str(rid_dup): ["앱", "앱", "로그인"]})
        uniq, uniq_session = _make_rule_assigner(self.ORGS, {str(rid_uniq): ["앱", "로그인"]})

        assert dup.assign(dup_session, rid_dup).confidence == pytest.approx(
            uniq.assign(uniq_session, rid_uniq).confidence
        )

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
    """리비전 20260813_0002 가 적용된 세션.

    적용은 conftest 의 스키마 fixture 가 `alembic upgrade head` 로 수행한다
    (schema_v4.sql 은 베이스라인 스냅샷이라 이후 리비전의 컬럼/제약이 없다).
    이 fixture 는 그 사실을 이름으로 드러내기 위해 남긴다.
    """
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
                "AND conname = 'uq_reviews_assigned_review_id_assigner'"
            )
        ).fetchone()

        assert constraint is not None


# ─────────────────────────────────────────────
# G. 배치 실행 (실제 PostgreSQL)
# ─────────────────────────────────────────────

class _ExplodingAssigner(RuleBasedAssigner):
    """지정한 review_id 에서만 DB 오류를 일으키는 배정기.

    assign() 이 하는 일은 사실상 DB 질의뿐이라 현실적인 실패 모드가 DB 오류다.
    세션을 실제로 실패 상태로 만들어야 복구 경로를 검증할 수 있다.
    """

    def __init__(self, boom_id):
        self.logger = MagicMock()
        self.db_connector = MagicMock()
        self.retriever = OrgCandidateRetriever()
        self._boom_id = boom_id

    def assign(self, session, review_id):
        if review_id == self._boom_id:
            session.execute(text("SELECT 1 FROM table_that_does_not_exist"))
        return super().assign(session, review_id)


@pytest.mark.requires_db
@pytest.mark.integration
class TestAssignBatch:
    def _seed(self, session, count=3, day=None):
        now = datetime.now(timezone.utc) if day is None else day
        app = App(
            app_id=uuid7(),
            platform_app_id=f"batch-{uuid7()}",
            platform_type=PlatformType.PLAYSTORE,
            name="Batch Test App",
        )
        session.add(app)
        reviews = []
        for i in range(count):
            r = ReviewMasterIndex(
                review_id=uuid7(),
                app_id=app.app_id,
                platform_review_id=f"batch-review-{uuid7()}",
                platform_type=PlatformType.PLAYSTORE,
                review_created_at=now,
                ingested_at=now,
                processing_status=ProcessingStatusType.ANALYZED,
                is_active=True,
                is_reply=False,
            )
            reviews.append(r)
        session.add_all(reviews)
        session.flush()
        return [r.review_id for r in reviews]

    def _assigner(self, session):
        assigner = RuleBasedAssigner.__new__(RuleBasedAssigner)
        assigner.logger = MagicMock()
        assigner.db_connector = MagicMock()
        assigner.db_connector.get_session.return_value = session
        assigner.retriever = OrgCandidateRetriever()
        return assigner

    def test_batch_assigns_and_counts(self, reviews_assigned_unique):
        session = reviews_assigned_unique
        ids = self._seed(session, 3)

        result = self._assigner(session).assign_batch(batch_size=2)

        assert result["total"] == 3
        assert result["failed"] == 0
        assert result["assigned"] + result["unclassified"] == 3
        assert len(fetch_assignments(session, ids)) == 3

    def test_already_assigned_reviews_are_skipped(self, reviews_assigned_unique):
        session = reviews_assigned_unique
        self._seed(session, 3)
        assigner = self._assigner(session)

        assigner.assign_batch()
        second = assigner.assign_batch()

        # 재실행이 과거 전량을 다시 훑으면 DAG 에서 매일 전량 재처리가 된다.
        assert second["total"] == 0

    def test_reassign_flag_reprocesses(self, reviews_assigned_unique):
        session = reviews_assigned_unique
        self._seed(session, 2)
        assigner = self._assigner(session)

        assigner.assign_batch()

        assert assigner.assign_batch(reassign=True)["total"] == 2

    def test_target_date_scopes_the_batch(self, reviews_assigned_unique):
        session = reviews_assigned_unique
        old = datetime(2020, 1, 1, tzinfo=timezone.utc)
        self._seed(session, 2, day=old)
        self._seed(session, 3)

        result = self._assigner(session).assign_batch(target_date=old.date())

        assert result["total"] == 2

    def test_db_error_on_one_review_is_recorded_and_batch_continues(
        self, reviews_assigned_unique
    ):
        session = reviews_assigned_unique
        ids = self._seed(session, 3)
        assigner = _ExplodingAssigner(ids[1])
        assigner.db_connector.get_session.return_value = session

        result = assigner.assign_batch()

        assert result["total"] == 3
        assert result["failed"] == 1
        stored = fetch_assignments(session, ids)
        # 실패 행이 실제로 남아야 한다. 세션이 오염되면 이 행이 사라지고
        # 나머지 리뷰도 처리되지 않는다.
        assert stored[str(ids[1])].is_failed is True
        assert len(stored) == 3


@pytest.mark.requires_db
@pytest.mark.integration
class TestAssignerDiscriminator:
    def test_two_assigners_coexist_for_one_review(
        self, reviews_assigned_unique, assigned_review_id
    ):
        session = reviews_assigned_unique
        review_id = assigned_review_id

        save_assignment(session, Assignment(
            review_id=review_id, assigned_dept=["1"], assignment_reason="규칙",
            confidence=0.4, assigner="rule"))
        save_assignment(session, Assignment(
            review_id=review_id, assigned_dept=["1", "1-1"], assignment_reason="LLM",
            confidence=0.9, assigner="llm"))
        session.flush()

        # 판별 컬럼이 없으면 두 번째가 첫 번째를 덮어써 비교 대상이 사라진다.
        assert fetch_assignments(session, [review_id], assigner="rule")[
            str(review_id)].assigned_dept == ["1"]
        assert fetch_assignments(session, [review_id], assigner="llm")[
            str(review_id)].assigned_dept == ["1", "1-1"]

    def test_empty_id_list_returns_nothing(self, reviews_assigned_unique, assigned_review_id):
        session = reviews_assigned_unique
        save_assignment(session, Assignment(
            review_id=assigned_review_id, assigned_dept=["1"],
            assignment_reason="r", confidence=0.5))
        session.flush()

        # 빈 목록을 '필터 없음'으로 취급하면 전체 스캔으로 조용히 넓어진다.
        assert fetch_assignments(session, []) == {}
        assert len(fetch_assignments(session, None)) >= 1


class TestEvalFailedKind:
    def test_failed_assignment_is_not_counted_as_abstention(self):
        labels = [Label(review_id="a", org_id="1-1")]
        assignments = {"a": Assignment(
            review_id="a", assigned_dept=[UNCLASSIFIED],
            assignment_reason="RuntimeError: boom", confidence=0.0, is_failed=True)}

        result = evaluate(labels, assignments)

        assert result.counts == {"failed": 1}
        assert result.failed_rate == pytest.approx(1.0)
        assert result.unclassified_rate == 0.0


class TestDuplicateLabels:
    def test_duplicate_review_id_raises(self, tmp_path):
        path = tmp_path / "dup.csv"
        path.write_text(
            "no,review_id,service,platform,rating,review_text,assigned_dept,"
            "stopped_at_parent,ambiguous,memo\n"
            "1,rid-a,신한,PLAYSTORE,1,본문,1-1,,,\n"
            "2,rid-a,신한,PLAYSTORE,1,본문,1-2,,,\n",
            encoding="utf-8",
        )

        with pytest.raises(ValueError, match="중복"):
            load_labels(path)


# ─────────────────────────────────────────────
# H. 2차 리뷰 대응 — 배정기 식별자와 실패 재시도
# ─────────────────────────────────────────────

class TestAssignerIdentityIsPinned:
    """ASSIGNER 값이 마이그레이션 server_default 와 어긋나면 조용히 과거 전량이
    재처리된다. 내부 일관성만으로는 드리프트가 드러나지 않으므로 값을 고정한다."""

    def test_rule_assigner_matches_migration_default(self):
        revision = _load_revision_module()

        assert RuleBasedAssigner.ASSIGNER == ASSIGNER_RULE
        assert revision.DEFAULT_ASSIGNER == ASSIGNER_RULE

    def test_migration_server_default_matches_orm_model(self):
        from src.models.review_assigned import ReviewAssigned

        column_default = ReviewAssigned.__table__.c.assigner.server_default.arg
        assert str(column_default).strip("'") == ASSIGNER_RULE

    def test_assignment_carries_the_assigner_of_its_producer(self):
        rid = uuid7()
        assigner, session = _make_rule_assigner(
            TestOrgCandidateRetriever.ORGS, {str(rid): ["로그인"]}
        )

        assert assigner.assign(session, rid).assigner == ASSIGNER_RULE


class TestFetchAssignmentsMixing:
    def test_unfiltered_fetch_refuses_to_pick_between_assigners(self):
        """review_id 로 키잉된 dict 는 배정기별 행을 담을 수 없다. 조용히 하나를
        버리면 비교 리포트가 비결정적이 된다."""
        session = MagicMock()
        session.execute.return_value.fetchall.return_value = [
            (uuid7(), ["1"], "규칙", 0.4, False, 1, "rule"),
        ]
        rows = session.execute.return_value.fetchall.return_value
        rid = rows[0][0]
        session.execute.return_value.fetchall.return_value = [
            (rid, ["1"], "규칙", 0.4, False, 1, "rule"),
            (rid, ["1", "1-1"], "LLM", 0.9, False, 1, "llm"),
        ]

        with pytest.raises(ValueError, match="배정기가 둘 이상"):
            fetch_assignments(session)

    def test_empty_id_list_does_not_touch_the_database(self):
        """가드의 의도는 반환값이 아니라 DB 왕복 회피다."""
        session = MagicMock()

        assert fetch_assignments(session, []) == {}
        session.execute.assert_not_called()


@pytest.mark.requires_db
@pytest.mark.integration
class TestFailedRowsAreRetried:
    def _seed(self, session, count=1):
        now = datetime.now(timezone.utc)
        app = App(app_id=uuid7(), platform_app_id=f"retry-{uuid7()}",
                  platform_type=PlatformType.PLAYSTORE, name="Retry App")
        session.add(app)
        rs = [ReviewMasterIndex(
            review_id=uuid7(), app_id=app.app_id,
            platform_review_id=f"retry-review-{uuid7()}",
            platform_type=PlatformType.PLAYSTORE, review_created_at=now,
            ingested_at=now, processing_status=ProcessingStatusType.ANALYZED,
            is_active=True, is_reply=False) for _ in range(count)]
        session.add_all(rs)
        session.flush()
        return [r.review_id for r in rs]

    def _assigner(self, session):
        a = RuleBasedAssigner.__new__(RuleBasedAssigner)
        a.logger = MagicMock()
        a.db_connector = MagicMock()
        a.db_connector.get_session.return_value = session
        a.retriever = OrgCandidateRetriever()
        return a

    def test_failed_review_is_picked_up_again(self, reviews_assigned_unique):
        session = reviews_assigned_unique
        review_id = self._seed(session)[0]

        save_assignment(session, unclassified(review_id, "DB 오류", failed=True))
        session.flush()

        # 실패 행을 '배정됨'으로 세면 이 리뷰는 영구히 사라진다.
        assert self._assigner(session).assign_batch()["total"] == 1

    def test_retry_stops_at_max_tries(self, reviews_assigned_unique):
        session = reviews_assigned_unique
        review_id = self._seed(session)[0]
        assigner = self._assigner(session)

        session.execute(
            text(
                "INSERT INTO reviews_assigned "
                "(review_id, assigner, assigned_dept, assignment_reason, "
                " confidence, is_failed, try_number) "
                "VALUES (:rid, :a, ARRAY['미분류'], '반복 실패', 0.0, true, :n)"
            ),
            {"rid": str(review_id), "a": assigner.ASSIGNER, "n": assigner.MAX_TRIES},
        )
        session.flush()

        # 상한에 닿으면 무한 재시도를 멈춘다.
        assert assigner.assign_batch()["total"] == 0
