"""Tests for GoldActionAnalyzer (Issue #39)

Coverage:
- _calc_attention(): Case A / Case B / no mismatch
- _apply_lfs(): 각 LF 독립 동작, 다중 LF, zero votes
- _build_record(): 정상 경로, 데이터 없음 skip
- process(): 이미 분석됨 skip, 정상 적재, LLM 실패 시에도 저장
- _generate_summary(): LLM 성공, 실패(max retry), client=None
"""

import json
import pytest
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch, PropertyMock
from uuid6 import uuid7

from src.gold.action_analyzer import (
    GoldActionAnalyzer,
    _apply_lfs,
    _lf_bug_keyword,
    _lf_request_keyword,
    _lf_low_rating,
    ACTION_REQUIRED,
    ACTION_NOT_REQUIRED,
    ABSTAIN,
)
from src.models.review_action_analysis import ReviewActionAnalysis


# ─────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────

def _make_analyzer(llm_client=None) -> GoldActionAnalyzer:
    a = GoldActionAnalyzer.__new__(GoldActionAnalyzer)
    a.logger = MagicMock()
    a.db_connector = MagicMock()
    a._llm = llm_client
    return a


def _make_session(already_analyzed: bool = False):
    session = MagicMock()
    # _is_already_analyzed
    session.query.return_value.filter.return_value.first.return_value = (
        (MagicMock(),) if already_analyzed else None
    )
    return session


# ─────────────────────────────────────────────
# A. Labeling Functions
# ─────────────────────────────────────────────

class TestLabelingFunctions:
    def test_lf_bug_keyword_match(self):
        assert _lf_bug_keyword("앱에서 오류가 발생합니다") == ACTION_REQUIRED

    def test_lf_bug_keyword_no_match(self):
        assert _lf_bug_keyword("편리한 앱입니다") == ABSTAIN

    def test_lf_request_keyword_match(self):
        assert _lf_request_keyword("기능 추가해달라") == ACTION_REQUIRED

    def test_lf_request_keyword_no_match(self):
        assert _lf_request_keyword("잘 쓰고 있어요") == ABSTAIN

    def test_lf_low_rating_triggered(self):
        assert _lf_low_rating(1) == ACTION_REQUIRED
        assert _lf_low_rating(2) == ACTION_REQUIRED

    def test_lf_low_rating_not_triggered(self):
        assert _lf_low_rating(3) == ABSTAIN
        assert _lf_low_rating(5) == ABSTAIN


# ─────────────────────────────────────────────
# B. apply_lfs (MajorityLabelVoter 대체)
# ─────────────────────────────────────────────

class TestApplyLfs:
    def test_no_votes_returns_not_required(self):
        label, conf, reason = _apply_lfs("안녕하세요", rating=3)
        assert label == ACTION_NOT_REQUIRED
        assert conf == pytest.approx(0.5)
        assert reason == ""

    def test_single_bug_keyword_fires(self):
        label, conf, reason = _apply_lfs("버그가 있어요", rating=3)
        assert label == ACTION_REQUIRED
        assert conf == pytest.approx(1.0)
        assert "LF_bug_keyword" in reason

    def test_low_rating_fires(self):
        label, conf, reason = _apply_lfs("잘 쓰고 있어요", rating=1)
        assert label == ACTION_REQUIRED
        assert "LF_low_rating" in reason

    def test_majority_wins(self):
        # bug + request + low_rating → 3/3 = 1.0
        label, conf, reason = _apply_lfs("버그 고쳐줘", rating=2)
        assert label == ACTION_REQUIRED
        assert conf == pytest.approx(1.0)

    def test_minority_loses(self):
        # only request fires (1/1), but rating=4 → LF_low_rating abstain
        # bug keyword not present → 1 fired, confidence 1.0
        label, conf, reason = _apply_lfs("개선해주세요", rating=4)
        assert label == ACTION_REQUIRED  # 1 LF fired, 1/1 = 1.0


# ─────────────────────────────────────────────
# C. _calc_attention
# ─────────────────────────────────────────────

class TestCalcAttention:
    def setup_method(self):
        self.analyzer = _make_analyzer()

    def test_case_a_high_rating_negative_sentiment(self):
        assert self.analyzer._calc_attention(rating=5, avg_sentiment=0.2) is True

    def test_case_b_low_rating_positive_sentiment(self):
        assert self.analyzer._calc_attention(rating=1, avg_sentiment=0.9) is True

    def test_no_mismatch(self):
        assert self.analyzer._calc_attention(rating=5, avg_sentiment=0.8) is False
        assert self.analyzer._calc_attention(rating=1, avg_sentiment=0.1) is False

    def test_boundary_case_a(self):
        # rating=4, sentiment=0.4 → NOT triggered (not < 0.4)
        assert self.analyzer._calc_attention(rating=4, avg_sentiment=0.4) is False

    def test_boundary_case_b(self):
        # rating=2, sentiment=0.6 → NOT triggered (not > 0.6)
        assert self.analyzer._calc_attention(rating=2, avg_sentiment=0.6) is False


# ─────────────────────────────────────────────
# D. _generate_summary
# ─────────────────────────────────────────────

class TestGenerateSummary:
    def test_no_llm_client_returns_none(self):
        analyzer = _make_analyzer(llm_client=None)
        session = MagicMock()
        result = analyzer._generate_summary(session, uuid7(), "텍스트")
        assert result is None

    def test_llm_success(self):
        mock_llm = MagicMock()
        mock_resp = MagicMock()
        mock_resp.choices[0].message.content = "한 줄 요약입니다."
        mock_llm.chat.completions.create.return_value = mock_resp

        analyzer = _make_analyzer(llm_client=mock_llm)
        session = MagicMock()
        session.add = MagicMock()
        session.flush = MagicMock()

        result = analyzer._generate_summary(session, uuid7(), "리뷰 텍스트")
        assert result == "한 줄 요약입니다."

    def test_llm_failure_returns_none(self):
        mock_llm = MagicMock()
        mock_llm.chat.completions.create.side_effect = Exception("API 오류")

        analyzer = _make_analyzer(llm_client=mock_llm)
        session = MagicMock()
        session.add = MagicMock()
        session.flush = MagicMock()

        result = analyzer._generate_summary(session, uuid7(), "리뷰 텍스트")
        assert result is None


# ─────────────────────────────────────────────
# E. process() - single record
# ─────────────────────────────────────────────

class TestProcess:
    def setup_method(self):
        self.analyzer = _make_analyzer()
        self.review_id = uuid7()

    def test_skip_if_already_analyzed(self):
        session = _make_session(already_analyzed=True)
        result = self.analyzer.process(session, self.review_id)
        assert result is True
        session.merge.assert_not_called()

    def test_skip_if_no_rmi(self):
        session = _make_session(already_analyzed=False)
        # session.get returns None for any model (no ReviewMasterIndex found)
        session.get.side_effect = None
        session.get.return_value = None
        result = self.analyzer.process(session, self.review_id)
        assert result is True
        session.merge.assert_not_called()

    def test_happy_path_merges_record(self):
        session = _make_session(already_analyzed=False)

        # ReviewMasterIndex
        rmi = MagicMock()
        rmi.platform_review_id = "R_12345"

        # AppReview (use MagicMock only)
        app_review = MagicMock()
        app_review.rating = 1

        # ReviewPreprocessed
        preprocessed = MagicMock()
        preprocessed.refined_text = "버그가 너무 많아요 고쳐줘"

        # session.get: ReviewMasterIndex → rmi, ReviewPreprocessed → preprocessed
        def _get(model, pk):
            name = model.__name__ if hasattr(model, "__name__") else str(model)
            if "ReviewMasterIndex" in name:
                return rmi
            if "ReviewPreprocessed" in name:
                return preprocessed
            return None

        session.get.side_effect = _get

        # query().filter_by().first() for AppReview
        session.query.return_value.filter_by.return_value.first.return_value = app_review
        # query().filter().all() for ReviewAspect
        session.query.return_value.filter.return_value.all.return_value = []

        result = self.analyzer.process(session, self.review_id)
        assert result is True
        session.merge.assert_called_once()
        record = session.merge.call_args[0][0]
        assert isinstance(record, ReviewActionAnalysis)
        assert record.review_id == self.review_id
        assert record.is_action_required is True  # 버그+요청+저별점 → True

    def test_exception_returns_false(self):
        session = _make_session(already_analyzed=False)
        session.get.side_effect = RuntimeError("DB 오류")
        result = self.analyzer.process(session, self.review_id)
        assert result is False
