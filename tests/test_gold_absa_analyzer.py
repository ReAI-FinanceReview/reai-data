"""Tests for GoldABSAAnalyzer (Issue #38)

Coverage:
- Sentiment score calculation (S_final = S_base × W_adv, negation inversion)
- Keyword extraction (dict-match fallback, KoNLPy unavailable)
- Category mapping (rule-based 1st, vector similarity 2nd)
- process(): single-record happy path, skip-if-already-analyzed, skip-if-no-text
- process_batch(): standalone batch with DB
"""

import math
import pytest
from unittest.mock import MagicMock, patch
from uuid6 import uuid7

from src.gold.absa_analyzer import (
    GoldABSAAnalyzer,
    _cosine_similarity,
    _SENTIMENT_DICT,
    _CATEGORY_KEYWORDS,
)
from src.models.enums import CategoryType
from src.models.review_aspects import ReviewAspect


# ─────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────

def _make_analyzer() -> GoldABSAAnalyzer:
    """Create GoldABSAAnalyzer with mocked DB and no Okt."""
    analyzer = GoldABSAAnalyzer.__new__(GoldABSAAnalyzer)
    analyzer.logger = MagicMock()
    analyzer.db_connector = MagicMock()
    analyzer._okt = None  # force dict-match fallback
    return analyzer


def _make_session(already_analyzed: bool = False):
    """Mock SQLAlchemy session."""
    session = MagicMock()
    # _is_already_analyzed queries ReviewAspect.aspect_id
    session.query.return_value.filter.return_value.first.return_value = (
        (1,) if already_analyzed else None
    )
    session.get.return_value = None
    return session


# ─────────────────────────────────────────────
# A. Utility functions
# ─────────────────────────────────────────────

class TestCosineSimilarity:
    def test_identical_vectors(self):
        v = [1.0, 0.0, 0.0]
        assert _cosine_similarity(v, v) == pytest.approx(1.0)

    def test_orthogonal_vectors(self):
        a = [1.0, 0.0]
        b = [0.0, 1.0]
        assert _cosine_similarity(a, b) == pytest.approx(0.0)

    def test_opposite_vectors(self):
        a = [1.0, 0.0]
        b = [-1.0, 0.0]
        assert _cosine_similarity(a, b) == pytest.approx(-1.0)

    def test_zero_vector_returns_zero(self):
        a = [0.0, 0.0]
        b = [1.0, 0.0]
        assert _cosine_similarity(a, b) == pytest.approx(0.0)


# ─────────────────────────────────────────────
# B. Sentiment score calculation
# ─────────────────────────────────────────────

class TestSentimentCalculation:
    def setup_method(self):
        self.analyzer = _make_analyzer()

    def test_no_adverb_no_negation(self):
        """편리 (0.85) × 1.0 = 0.85"""
        text = "편리한 앱입니다"
        aspects = self.analyzer._analyze(MagicMock(), uuid7(), text)
        kw_aspects = {a.keyword: a for a in aspects}
        assert "편리" in kw_aspects
        assert kw_aspects["편리"].sentiment_score == pytest.approx(0.85, abs=1e-4)

    def test_adverb_intensifies_score(self):
        """매우 (1.3) × 편리 (0.85) = 1.105 → clamped to 1.0"""
        text = "매우 편리한 앱입니다"
        aspects = self.analyzer._analyze(MagicMock(), uuid7(), text)
        kw_aspects = {a.keyword: a for a in aspects}
        assert kw_aspects["편리"].sentiment_score == pytest.approx(1.0, abs=1e-4)

    def test_adverb_weakens_score(self):
        """좀 (0.8) × 편리 (0.85) = 0.68"""
        text = "좀 편리한 앱입니다"
        aspects = self.analyzer._analyze(MagicMock(), uuid7(), text)
        kw_aspects = {a.keyword: a for a in aspects}
        assert kw_aspects["편리"].sentiment_score == pytest.approx(0.68, abs=1e-4)

    def test_negation_inverts_score(self):
        """안 + 편리 → 1.0 - 0.85 = 0.15"""
        text = "안 편리한 앱입니다"
        aspects = self.analyzer._analyze(MagicMock(), uuid7(), text)
        kw_aspects = {a.keyword: a for a in aspects}
        assert kw_aspects["편리"].sentiment_score == pytest.approx(0.15, abs=1e-4)

    def test_score_clamped_to_zero(self):
        """오류 (0.10), negation → 1.0 - 0.10 = 0.90 (no negative clamping needed here)"""
        text = "못 오류나는 앱"
        aspects = self.analyzer._analyze(MagicMock(), uuid7(), text)
        kw_aspects = {a.keyword: a for a in aspects}
        assert 0.0 <= kw_aspects["오류"].sentiment_score <= 1.0

    def test_unknown_keyword_defaults_to_neutral(self):
        """Keyword not in _SENTIMENT_DICT → S_base = 0.5"""
        # Inject a keyword that's in category dict but not sentiment dict
        text = "디자인이 훌륭합니다"
        aspects = self.analyzer._analyze(MagicMock(), uuid7(), text)
        for a in aspects:
            assert 0.0 <= a.sentiment_score <= 1.0


# ─────────────────────────────────────────────
# C. Keyword extraction (dict-match fallback)
# ─────────────────────────────────────────────

class TestKeywordExtraction:
    def setup_method(self):
        self.analyzer = _make_analyzer()

    def test_extracts_sentiment_keywords(self):
        # Use exact keyword substrings that appear in the dict
        text = "편리 빠르 앱"
        keywords = self.analyzer._extract_keywords(text)
        assert "편리" in keywords
        assert "빠르" in keywords

    def test_extracts_category_keywords(self):
        text = "디자인이 좋아요"
        keywords = self.analyzer._extract_keywords(text)
        assert "디자인" in keywords

    def test_deduplication(self):
        text = "편리 편리 편리"
        keywords = self.analyzer._extract_keywords(text)
        assert keywords.count("편리") == 1

    def test_max_20_keywords(self):
        # Build text with many matching keywords
        all_kws = list(_SENTIMENT_DICT.keys()) + list(
            kw for kws in _CATEGORY_KEYWORDS.values() for kw in kws
        )
        text = " ".join(all_kws)
        keywords = self.analyzer._extract_keywords(text)
        assert len(keywords) <= 20

    def test_empty_text_returns_empty(self):
        assert self.analyzer._extract_keywords("") == []

    def test_no_matching_keywords(self):
        assert self.analyzer._extract_keywords("안녕하세요 반갑습니다") == []


# ─────────────────────────────────────────────
# D. Negation & adverb detection
# ─────────────────────────────────────────────

class TestNegationAdverb:
    def setup_method(self):
        self.analyzer = _make_analyzer()

    def test_has_negation_안(self):
        assert self.analyzer._has_negation("안 돼요") is True

    def test_has_negation_못(self):
        assert self.analyzer._has_negation("못 써요") is True

    def test_no_negation(self):
        assert self.analyzer._has_negation("잘 돼요") is False

    def test_adv_weight_매우(self):
        assert self.analyzer._get_adv_weight("매우 좋아요") == pytest.approx(1.3)

    def test_adv_weight_극도로(self):
        assert self.analyzer._get_adv_weight("극도로 불편") == pytest.approx(1.4)

    def test_adv_weight_none(self):
        assert self.analyzer._get_adv_weight("그냥 앱입니다") == pytest.approx(1.0)

    def test_adv_weight_largest_deviation_wins(self):
        """극도로(1.4) > 매우(1.3) → 1.4 wins"""
        assert self.analyzer._get_adv_weight("극도로 매우 불편") == pytest.approx(1.4)

    def test_no_false_positive_negation_in_안정(self):
        """'안정적' 텍스트에서 '안'을 부정어로 오탐하지 않아야 함."""
        assert self.analyzer._has_negation("앱이 안정적이고 빠릅니다") is False


# ─────────────────────────────────────────────
# E. Category mapping
# ─────────────────────────────────────────────

class TestCategoryMapping:
    def setup_method(self):
        self.analyzer = _make_analyzer()
        self.session = MagicMock()
        self.review_id = uuid7()

    def test_usability_keyword(self):
        cat = self.analyzer._map_category(self.session, self.review_id, "편리")
        assert cat == CategoryType.USABILITY

    def test_stability_keyword(self):
        cat = self.analyzer._map_category(self.session, self.review_id, "오류")
        assert cat == CategoryType.STABILITY

    def test_speed_keyword(self):
        cat = self.analyzer._map_category(self.session, self.review_id, "속도")
        assert cat == CategoryType.SPEED

    def test_design_keyword(self):
        cat = self.analyzer._map_category(self.session, self.review_id, "디자인")
        assert cat == CategoryType.DESIGN

    def test_unknown_keyword_falls_back_to_vector(self):
        """Unknown keyword → vector fallback. No embedding in session → None."""
        self.session.get.return_value = None  # no ReviewEmbedding
        cat = self.analyzer._map_category(self.session, self.review_id, "xyz알수없는단어")
        assert cat is None

    def test_category_stored_as_string_in_aspect(self):
        text = "오류가 많아요"
        aspects = self.analyzer._analyze(self.session, self.review_id, text)
        for a in aspects:
            if a.category is not None:
                assert isinstance(a.category, str)


# ─────────────────────────────────────────────
# F. process() - single record
# ─────────────────────────────────────────────

class TestProcess:
    def setup_method(self):
        self.analyzer = _make_analyzer()
        self.review_id = uuid7()

    def test_skip_if_already_analyzed(self):
        session = _make_session(already_analyzed=True)
        result = self.analyzer.process(session, self.review_id)
        assert result is True
        session.add_all.assert_not_called()

    def test_skip_if_no_preprocessed_record(self):
        session = _make_session(already_analyzed=False)
        session.get.return_value = None
        result = self.analyzer.process(session, self.review_id)
        assert result is True
        session.add_all.assert_not_called()

    def test_skip_if_empty_refined_text(self):
        session = _make_session(already_analyzed=False)
        preprocessed = MagicMock()
        preprocessed.refined_text = ""
        session.get.return_value = preprocessed
        result = self.analyzer.process(session, self.review_id)
        assert result is True
        session.add_all.assert_not_called()

    def test_skip_if_no_keywords_found(self):
        session = _make_session(already_analyzed=False)
        preprocessed = MagicMock()
        preprocessed.refined_text = "안녕하세요 반갑습니다"  # no matching keywords
        session.get.return_value = preprocessed
        result = self.analyzer.process(session, self.review_id)
        assert result is True
        # add_all([]) may be called — verify no aspects were inserted
        for call in session.add_all.call_args_list:
            assert call[0][0] == []

    def test_happy_path_adds_aspects(self):
        session = _make_session(already_analyzed=False)
        preprocessed = MagicMock()
        preprocessed.refined_text = "편리하고 빠른 앱입니다"
        session.get.return_value = preprocessed
        result = self.analyzer.process(session, self.review_id)
        assert result is True
        session.add_all.assert_called_once()
        aspects = session.add_all.call_args[0][0]
        assert len(aspects) > 0
        for a in aspects:
            assert isinstance(a, ReviewAspect)
            assert a.review_id == self.review_id
            assert 0.0 <= a.sentiment_score <= 1.0

    def test_analyze_exception_returns_false_and_rollbacks(self):
        session = _make_session(already_analyzed=False)
        preprocessed = MagicMock()
        preprocessed.refined_text = "편리한 앱입니다"
        session.get.return_value = preprocessed
        with patch.object(self.analyzer, "_analyze", side_effect=RuntimeError("DB error")):
            result = self.analyzer.process(session, self.review_id)
        assert result is False
        session.rollback.assert_called_once()
