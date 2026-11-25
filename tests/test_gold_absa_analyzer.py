"""Tests for GoldABSAAnalyzer (Issue #38)

Coverage:
- Sentiment score calculation (S_final = S_base × W_adv, negation inversion)
- Keyword extraction (dict-match fallback, KoNLPy unavailable)
- Category mapping (rule-based 1st, vector similarity 2nd)
- process(): single-record happy path, skip-if-already-analyzed, required-row failures
- process_batch(): standalone batch with DB
"""

import pytest
from unittest.mock import MagicMock, patch
from uuid6 import uuid7

from src.gold.absa_analyzer import (
    GoldABSAAnalyzer,
    _cosine_similarity,
    _SENTIMENT_DICT,
    _CATEGORY_KEYWORDS,
    _COMPACT_NEGATION_PREFIXES,
    _KEYWORD_TO_CATEGORY,
    _NEGATION_WORDS,
    _PHRASE_KEYWORDS,
    _PREDICATE_LEXICON,
    _SATISFIED_USAGE_RE,
    _skip_reason_before_extraction,
)
from src.gold.skip import AnalysisSkipped
from src.models.enums import CategoryType, ProcessingStatusType
from src.models.review_aspects import ReviewAspect


# ─────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────

def _make_analyzer(okt_mock=None) -> GoldABSAAnalyzer:
    """Create GoldABSAAnalyzer with mocked DB and optional Okt mock."""
    analyzer = GoldABSAAnalyzer.__new__(GoldABSAAnalyzer)
    analyzer.logger = MagicMock()
    analyzer.db_connector = MagicMock()
    analyzer._okt = okt_mock  # None forces dict-match fallback
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

    def test_negation_applies_only_to_nearby_keyword(self):
        """혼합 감성: '편리한 앱인데 그냥 쓰기 좋고 오류는 안 나요'
        편리(token 0): window[0:4] = "편리한 앱인데 그냥 쓰기" → 부정어/부사 없음 → 0.85 유지
        오류(token 5): window[2:9] = "그냥 쓰기 좋고 오류는 안 나요" → "안" 포함 → 반전 0.90
        """
        text = "편리한 앱인데 그냥 쓰기 좋고 오류는 안 나요"
        aspects = self.analyzer._analyze(MagicMock(), uuid7(), text)
        kw_aspects = {a.keyword: a for a in aspects}
        assert "편리" in kw_aspects
        assert "오류" in kw_aspects
        assert kw_aspects["편리"].sentiment_score == pytest.approx(0.85, abs=1e-4)
        assert kw_aspects["오류"].sentiment_score == pytest.approx(0.90, abs=1e-4)

    def test_adverb_applies_only_to_nearby_keyword(self):
        """혼합 강도: '매우 편리한 기능들 좋고 그런데 오류가 있어요'
        편리(token 1): window[0:5] = "매우 편리한 기능들 좋고 그런데" → "매우"(1.3) → 1.0
        오류(token 5): window[2:7] = "기능들 좋고 그런데 오류가 있어요" → "매우" 없음 → 0.10 유지
        """
        text = "매우 편리한 기능들 좋고 그런데 오류가 있어요"
        aspects = self.analyzer._analyze(MagicMock(), uuid7(), text)
        kw_aspects = {a.keyword: a for a in aspects}
        assert "편리" in kw_aspects
        assert "오류" in kw_aspects
        assert kw_aspects["편리"].sentiment_score == pytest.approx(1.0, abs=1e-4)
        assert kw_aspects["오류"].sentiment_score == pytest.approx(0.10, abs=1e-4)


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

    def test_okt_branch_success(self):
        """KoNLPy nouns returns known list → 2-char+ nouns used."""
        okt_mock = MagicMock()
        okt_mock.nouns.return_value = ["편리", "앱", "속도"]
        analyzer = _make_analyzer(okt_mock=okt_mock)
        keywords = analyzer._extract_keywords("편리한 앱 속도")
        assert "편리" in keywords
        assert "속도" in keywords
        assert "앱" not in keywords  # single char, filtered out

    def test_okt_branch_exception_falls_back_to_dict(self):
        """KoNLPy raises exception → fallback to dict-match."""
        okt_mock = MagicMock()
        okt_mock.nouns.side_effect = RuntimeError("okt error")
        analyzer = _make_analyzer(okt_mock=okt_mock)
        keywords = analyzer._extract_keywords("편리 빠르 앱")
        assert "편리" in keywords
        assert "빠르" in keywords

    def test_okt_empty_result_falls_back_to_dict(self):
        """KoNLPy가 짧은 활용형을 놓쳐도 사전 매칭으로 보완한다."""
        okt_mock = MagicMock()
        okt_mock.nouns.return_value = []
        analyzer = _make_analyzer(okt_mock=okt_mock)
        keywords = analyzer._extract_keywords("앱 좋아요")
        assert "좋" in keywords
        analyzer.logger.debug.assert_called_once_with(
            "Okt returned no usable keywords — fallback to dict match"
        )
        analyzer.logger.info.assert_not_called()

    @pytest.mark.parametrize(
        ("text", "expected_keyword"),
        [
            ("굿", "굿"),
            ("편하게", "편하"),
            ("감사합니다", "감사"),
            ("만족스럽습니다", "만족"),
        ],
    )
    def test_common_short_feedback_extracts_keyword(self, text, expected_keyword):
        """짧은 실사용 리뷰도 aspect row 생성에 필요한 키워드를 추출한다."""
        keywords = _make_analyzer()._extract_keywords(text)
        assert expected_keyword in keywords


# ─────────────────────────────────────────────
# D. Negation & adverb detection
# ─────────────────────────────────────────────

class TestNegationAdverb:
    def setup_method(self):
        self.analyzer = _make_analyzer()

    def test_compact_negation_prefixes_derive_from_negation_words(self):
        assert set(_COMPACT_NEGATION_PREFIXES) == _NEGATION_WORDS

    def test_has_negation_안(self):
        assert self.analyzer._has_negation("안 돼요") is True

    def test_has_negation_못(self):
        assert self.analyzer._has_negation("못 써요") is True

    def test_has_compact_negation_안좋음(self):
        assert self.analyzer._has_negation("안좋음", keyword="좋") is True

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

    def test_no_false_positive_negation_in_안녕하세요(self):
        """인사말의 '안'을 가까운 감성 키워드의 부정어로 오탐하지 않아야 함."""
        assert self.analyzer._has_negation("안녕하세요 좋은 앱입니다", keyword="좋") is False

    def test_compact_negation_inverts_sentiment(self):
        aspects = self.analyzer._analyze(MagicMock(), uuid7(), "안좋음")
        kw_aspects = {a.keyword: a for a in aspects}
        assert kw_aspects["좋"].sentiment_score == pytest.approx(0.20, abs=1e-4)


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

    def test_fails_if_no_preprocessed_record(self):
        session = _make_session(already_analyzed=False)
        session.get.return_value = None
        result = self.analyzer.process(session, self.review_id)
        assert result is False
        session.add_all.assert_not_called()
        assert session.get.call_count == 1

    def test_skips_empty_refined_text(self):
        """빈 refined_text(이모지만 있던 리뷰)는 재시도 대상이 아니라 종결 SKIPPED (#94)."""
        session = _make_session(already_analyzed=False)
        preprocessed = MagicMock()
        preprocessed.refined_text = ""
        session.get.return_value = preprocessed
        with pytest.raises(AnalysisSkipped) as excinfo:
            self.analyzer.process(session, self.review_id)
        assert excinfo.value.error_message == "skip:empty"
        session.add_all.assert_not_called()

    def test_fails_if_no_keywords_found_without_okt(self):
        """Okt 없이(용언 단계를 못 돌림) aspect 0건이면 SKIPPED 로 확정하지 않고 재시도 가능한 실패."""
        session = _make_session(already_analyzed=False)
        preprocessed = MagicMock()
        preprocessed.refined_text = "안녕하세요 반갑습니다"  # no matching keywords
        session.get.return_value = preprocessed
        result = self.analyzer.process(session, self.review_id)
        assert result is False
        session.add_all.assert_not_called()

    def test_happy_path_adds_aspects(self):
        session = _make_session(already_analyzed=False)
        preprocessed = MagicMock()
        preprocessed.refined_text = "편리하고 빠른 앱입니다"
        rmi_mock = MagicMock()
        # First get(ReviewPreprocessed) → preprocessed, second get(ReviewMasterIndex) → rmi_mock
        session.get.side_effect = [preprocessed, rmi_mock]
        result = self.analyzer.process(session, self.review_id)
        assert result is True
        session.add_all.assert_called_once()
        aspects = session.add_all.call_args[0][0]
        assert len(aspects) > 0
        for a in aspects:
            assert isinstance(a, ReviewAspect)
            assert a.review_id == self.review_id
            assert 0.0 <= a.sentiment_score <= 1.0
        assert rmi_mock.processing_status == ProcessingStatusType.ANALYZED

    def test_analyze_exception_returns_false_and_rollbacks(self):
        session = _make_session(already_analyzed=False)
        preprocessed = MagicMock()
        preprocessed.refined_text = "편리한 앱입니다"
        session.get.return_value = preprocessed
        with patch.object(self.analyzer, "_analyze", side_effect=RuntimeError("DB error")):
            result = self.analyzer.process(session, self.review_id)
        assert result is False
        session.begin_nested.assert_called_once()  # savepoint 사용 확인
        session.rollback.assert_not_called()        # 세션 전체 롤백 없음

    def test_integrity_error_treated_as_already_processed(self):
        """IntegrityError on insert → duplicate detected, return True (savepoint auto-rolled back)."""
        from sqlalchemy.exc import IntegrityError
        session = _make_session(already_analyzed=False)
        preprocessed = MagicMock()
        preprocessed.refined_text = "편리한 앱입니다"
        session.get.return_value = preprocessed
        # begin_nested context manager raises IntegrityError on __exit__
        cm = MagicMock()
        cm.__enter__.return_value = None
        cm.__exit__.side_effect = IntegrityError("UNIQUE constraint failed", None, None)
        session.begin_nested.return_value = cm
        result = self.analyzer.process(session, self.review_id)
        assert result is True
        session.rollback.assert_not_called()  # 세션 전체 롤백 없음


# ─────────────────────────────────────────────
# G. process_batch() - 혼합 성공/실패
# ─────────────────────────────────────────────

class TestProcessBatch:
    def setup_method(self):
        self.analyzer = _make_analyzer()

    def test_partial_failure_does_not_rollback_successful_reviews(self):
        session = MagicMock()
        self.analyzer.db_connector.get_session.return_value = session

        session.query.return_value.filter.return_value.first.return_value = None
        preprocessed = MagicMock()
        preprocessed.refined_text = "편리한 앱"
        session.get.return_value = preprocessed

        ids = [uuid7(), uuid7()]
        with patch.object(self.analyzer, "_fetch_pending_review_ids", return_value=ids), \
             patch.object(self.analyzer, "_analyze",
                          side_effect=[[MagicMock(spec=ReviewAspect)], RuntimeError("fail")]):
            count = self.analyzer.process_batch(batch_size=10)

        assert count == 1
        session.rollback.assert_not_called()  # 세션 전체 롤백 없어야 함
        session.commit.assert_called_once()   # 성공분은 커밋됨


# ─────────────────────────────────────────────
# H. 구어체 리뷰 용언 단계 (issue #94) — 실제 Okt
# ─────────────────────────────────────────────
# 아래 문장은 모두 실데이터에서 FAILED 로 남았던 리뷰다. 어휘는 실패 66건 밖의
# ANALYZED 리뷰에서 평점과 극성이 맞는지 확인한 것만 넣었다 (PR #118 본문의 case 분석).

@pytest.fixture(scope="module")
def okt_analyzer():
    from src.gold.absa_analyzer import KONLPY_AVAILABLE, Okt

    if not KONLPY_AVAILABLE:
        pytest.skip("konlpy is not installed")
    from src.utils.logger import get_logger

    analyzer = GoldABSAAnalyzer.__new__(GoldABSAAnalyzer)
    analyzer.logger = get_logger(__name__)
    analyzer.db_connector = None
    analyzer._okt = Okt()
    return analyzer


def _aspects_by_keyword(analyzer, session, text):
    # 임베딩이 없는 review_id 라 벡터 카테고리 폴백은 None 이 된다
    return {a.keyword: a for a in analyzer._analyze(session, uuid7(), text)}


class TestColloquialPredicateTier:
    @pytest.mark.parametrize(
        ("text", "keyword", "score", "category"),
        [
            # 부정 + 기능동사: 동작하지 않는다는 신고
            ("밥먹고 뭐하시나요? 앱이 안열리는데. 한심하네요.", "안열리", 0.10, "STABILITY"),
            ("안됨", "안되", 0.10, "STABILITY"),
            ("안됩니다", "안되", 0.10, "STABILITY"),
            ("안켜짐", "안켜지", 0.10, "STABILITY"),
            ("못쓰겠네", "못쓰", 0.15, None),
            # 감성 용언 활용형 → 사전 어간
            ("빨라요", "빠르", 0.80, "SPEED"),          # Okt 가 빨르다로 복원
            ("느려터졌음", "느리", 0.20, "SPEED"),
            ("귀찮아요 ㅠㅠ", "귀찮", 0.20, "USABILITY"),
            ("너무조아요", "좋", 0.96, None),           # 너무 × 1.2
            # 현재진행 사용 만족 구문
            ("잘 쓰고 있어요", "잘쓰", 0.80, None),
            ("잘쓰고있어요", "잘쓰", 0.80, None),
        ],
    )
    @pytest.mark.requires_db
    def test_rescues_previously_failed_reviews(
        self, test_db_session, okt_analyzer, text, keyword, score, category
    ):
        aspects = _aspects_by_keyword(okt_analyzer, test_db_session, text)
        assert keyword in aspects
        assert aspects[keyword].sentiment_score == pytest.approx(score, abs=1e-4)
        assert aspects[keyword].category == category

    @pytest.mark.requires_db
    def test_spaced_negation_is_not_flipped_back(self, test_db_session, okt_analyzer):
        """'안 돼요' 의 '안' 이 문맥 부정으로 다시 잡혀 0.90(긍정)이 되면 안 된다."""
        aspects = _aspects_by_keyword(okt_analyzer, test_db_session, "안 돼요")
        assert aspects["안되"].sentiment_score == pytest.approx(0.10, abs=1e-4)

    @pytest.mark.requires_db
    def test_surface_dictionary_keyword_keeps_window_negation(self, test_db_session, okt_analyzer):
        """표면 사전 단계 키워드는 기존 문맥 창 부정 규칙 그대로다: 안 좋아요 → 1 - 0.8."""
        aspects = _aspects_by_keyword(okt_analyzer, test_db_session, "안 좋아요")
        assert aspects["좋"].sentiment_score == pytest.approx(0.20, abs=1e-4)

    @pytest.mark.requires_db
    @pytest.mark.parametrize(
        ("text", "expected"),
        [
            # '안' 은 바로 뒤 용언만 부정한다. 문맥 창이면 느리·귀찮까지 뒤집혔다
            ("안 열리고 느려요", {"안열리": 0.10, "느리": 0.20}),
            ("안 켜지고 귀찮아요", {"안켜지": 0.10, "귀찮": 0.20}),
            # 붙여 쓴 부정: 토큰은 활용형(안빨라요)이고 키워드는 어간(빠르)이다
            ("안빨라요", {"빠르": 0.20}),
            ("안 빨라요", {"빠르": 0.20}),
            ("안쉬워요", {"쉽": 0.20}),
            ("안느려요", {"느리": 0.80}),
            ("안 귀찮아요", {"귀찮": 0.80}),
            # 장형 부정: Okt 는 '-지는/-진' 을 용언에 붙이고 '않다' 를 바로 다음 토큰으로 낸다
            ("귀찮지 않아요", {"귀찮": 0.80}),
            ("느려지지 않아요", {"느리": 0.80}),
            ("귀찮지는 않아요", {"귀찮": 0.80}),
            ("귀찮지 못해요", {"귀찮": 0.80}),         # 못 + 하다
            ("안 귀찮지 않아요", {"귀찮": 0.20}),      # 이중 부정은 상쇄된다
        ],
    )
    def test_predicate_negation_follows_the_adjacent_token(
        self, test_db_session, okt_analyzer, text, expected
    ):
        aspects = _aspects_by_keyword(okt_analyzer, test_db_session, text)
        assert {k: round(a.sentiment_score, 4) for k, a in aspects.items()} == expected

    @pytest.mark.parametrize(
        "text",
        [
            "잘 쓰고 있었는데 ㅠㅠ",  # 과거진행: 불만 리뷰 도입부 (ANALYZED 1★ 다수)
            "잘쓰던건데",              # Okt 합성어 잘쓰다: ANALYZED 2건 모두 1★
            "빨리 해주세요",           # 부사 빨리 ≠ 형용사 빠르다
            "안녕하세요 반갑습니다",   # #84 회귀: 인사말의 '안' 은 부정이 아니다
            "ㅇㅇ",
            "뭐지..",
            "짱",                      # 근거 1건뿐이라 넣지 않음
            "최곱니다",                # Okt 가 최 + 곱다로 분석
            "잘 쓰고 있지 않아요",     # 부정형: 구문 키워드는 반전하지 않으므로 아예 잡지 않는다
            "잘 쓰고 있지는 않아요",
            "잘 쓰고 있진 않아요",
            "잘 쓰고 있지도 않아요",
            "잘 쓰고 있지 못해요",
        ],
    )
    def test_extracts_nothing_for_rejected_or_meaningless_text(self, okt_analyzer, text):
        assert okt_analyzer._extract_keywords(text) == []

    @pytest.mark.parametrize("text", ["잘 쓰고 있지만 안돼요", "잘 쓰고 있던 거 안돼요"])
    def test_contrastive_usage_keeps_only_the_complaint(self, okt_analyzer, text):
        """대조·과거형 도입부는 '잘 쓰고 있는데 안돼요' 처럼 불만 키워드만 남는다."""
        assert okt_analyzer._extract_keywords(text) == ["안되"]

    def test_predicate_tier_does_not_run_when_nouns_exist(self, okt_analyzer):
        """명사가 나오는 리뷰는 기존 결과 그대로다 (용언 단계는 0건일 때만)."""
        keywords = okt_analyzer._extract_keywords("로그인이 안돼요")
        assert keywords == ["로그인"]

    def test_predicate_tier_does_not_run_when_dict_fallback_matches(self, okt_analyzer):
        """표면 사전이 잡는 짧은 리뷰도 기존 결과 그대로다 ('쉽게 할수있어 좋아요')."""
        keywords = okt_analyzer._extract_keywords("쉽게 할수있어 좋아요")
        assert sorted(keywords) == sorted(["좋", "쉽"])  # 사전 매칭 순서는 set 순회 순서

    def test_predicate_vocabulary_does_not_enter_the_surface_dictionary(self, okt_analyzer):
        """용언 단계 어휘(귀찮)는 표면 사전 결과를 바꾸지 않는다 (main 과 같이 [불편])."""
        assert okt_analyzer._extract_keywords("너무 귀찮고 불편해요") == ["불편"]

    def test_predicate_only_vocabulary_stays_out_of_the_surface_dictionary(self, okt_analyzer):
        """용언 단계 전용 어휘가 표면 사전에 들어가면 이미 성공한 리뷰의 결과가 바뀐다."""
        surface = set(_SENTIMENT_DICT) | set(_KEYWORD_TO_CATEGORY)
        predicate_only = set(_PREDICATE_LEXICON) | set(_PHRASE_KEYWORDS)
        assert predicate_only.isdisjoint(surface)
        for keyword in sorted(predicate_only):
            assert okt_analyzer._dict_match_keywords_with_spans(keyword) == [], keyword


class TestSatisfiedUsagePhrase:
    @pytest.mark.parametrize(
        "text", ["잘 쓰고 있어요", "잘쓰고있음", "잘 사용하고 있습니다", "잘사용중인데"]
    )
    def test_matches_present_progressive(self, text):
        assert _SATISFIED_USAGE_RE.search(text)

    @pytest.mark.parametrize(
        "text",
        [
            "잘 쓰고 있었는데 업데이트 후 안돼요",  # 과거진행
            "잘쓰던 기능인데",
            "잘 썼습니다",
            "잘 쓰고 있는데 안돼요",                # 대조: 불만 도입부
            "잘 쓰고 있다가 갑자기 안돼요",
            "잘 쓰고 있지 않아요",                  # 부정
            "잘 쓰고 있지는 않아요",
            "잘 쓰고 있진 않아요",
            "잘 쓰고 있지도 않아요",
            "잘 쓰고 있지 못해요",
            "잘 쓰고 있지만 안돼요",                # 대조
            "잘 쓰고 있으나 안돼요",
            "잘 쓰고 있던 거 안돼요",               # 과거
            "잘 사용 중이 아니에요",
        ],
    )
    def test_ignores_past_contrastive_and_negated_forms(self, text):
        assert _SATISFIED_USAGE_RE.search(text) is None


# ─────────────────────────────────────────────
# I. 분석 제외 판정 (issue #94)
# ─────────────────────────────────────────────

class TestSkipReasonBeforeExtraction:
    @pytest.mark.parametrize(
        ("text", "reason"),
        [
            (None, "empty"),
            ("", "empty"),
            ("   ", "empty"),
            ("Not working ipad .even iphone. I faced click issue", "non_korean"),
            ("Не могу зайти слетает", "non_korean"),
            ("挺好的", "non_korean"),
            ("[PROFANITY]", None),        # 마스킹 토큰만 남은 한국어 욕설 → 추출 단계에서 판정
            ("ㅇ", None),
            ("앱 good", None),
            ("12345 !!", None),
        ],
    )
    def test_reason(self, text, reason):
        assert _skip_reason_before_extraction(text) == reason


# ─────────────────────────────────────────────
# J. process() — 실제 DB (issue #94)
# ─────────────────────────────────────────────

def _seed_cleaned_review(session, refined_text):
    from sqlalchemy import text as sql

    app_id, review_id = uuid7(), uuid7()
    session.execute(
        sql(
            "INSERT INTO apps (app_id, platform_app_id, platform_type, name) "
            "VALUES (:app_id, :platform_app_id, 'APPSTORE', 'ABSA Skip Test App')"
        ),
        {"app_id": app_id, "platform_app_id": f"absa.skip.{app_id}"},
    )
    session.execute(
        sql(
            "INSERT INTO review_master_index "
            "(review_id, app_id, platform_review_id, platform_type, processing_status, retry_count) "
            "VALUES (:review_id, :app_id, :platform_review_id, 'APPSTORE', 'CLEANED', 0)"
        ),
        {"review_id": review_id, "app_id": app_id, "platform_review_id": f"absa-skip-{review_id}"},
    )
    session.execute(
        sql(
            "INSERT INTO reviews_preprocessed (review_id, platform_review_id, refined_text) "
            "VALUES (:review_id, :platform_review_id, :refined_text)"
        ),
        {
            "review_id": review_id,
            "platform_review_id": f"absa-skip-{review_id}",
            "refined_text": refined_text,
        },
    )
    session.flush()
    return review_id


def _aspect_count(session, review_id):
    return session.query(ReviewAspect).filter(ReviewAspect.review_id == review_id).count()


@pytest.mark.requires_db
class TestProcessSkipWithDatabase:
    @pytest.mark.parametrize(
        ("refined_text", "error_message"),
        [
            ("", "skip:empty"),
            ("The app is not user friendly and is constantly crashing.", "skip:non_korean"),
            ("ㅇㅇ", "skip:no_aspect"),
            ("[PROFANITY]", "skip:no_aspect"),
        ],
    )
    def test_unanalyzable_review_raises_skip_without_aspects(
        self, test_db_session, okt_analyzer, refined_text, error_message
    ):
        review_id = _seed_cleaned_review(test_db_session, refined_text)
        with pytest.raises(AnalysisSkipped) as excinfo:
            okt_analyzer.process(test_db_session, review_id)
        assert excinfo.value.error_message == error_message
        # #84 orphan 무결성: aspect 없는 리뷰를 ANALYZED 로 두지 않는다
        assert _aspect_count(test_db_session, review_id) == 0
        from src.models.review_master_index import ReviewMasterIndex

        rmi = test_db_session.get(ReviewMasterIndex, review_id)
        assert rmi.processing_status == ProcessingStatusType.CLEANED

    def test_outage_report_from_issue_is_analyzed(self, test_db_session, okt_analyzer):
        review_id = _seed_cleaned_review(
            test_db_session, "밥먹고 뭐하시나요? 앱이 안열리는데. 한심하네요."
        )
        assert okt_analyzer.process(test_db_session, review_id) is True
        test_db_session.flush()
        aspect = (
            test_db_session.query(ReviewAspect)
            .filter(ReviewAspect.review_id == review_id)
            .one()
        )
        assert (aspect.keyword, aspect.category) == ("안열리", "STABILITY")
        assert aspect.sentiment_score == pytest.approx(0.10, abs=1e-4)

    @pytest.mark.parametrize(
        "refined_text",
        [
            "로그인",
            # 정상이면 명사 단계에서 [로그인] 으로 끝나는 리뷰. 용언 단계의 [안되] 로 굳히지 않는다
            "로그인 안돼요",
        ],
    )
    def test_noun_step_failure_stays_retryable(self, test_db_session, okt_analyzer, refined_text):
        """Okt.nouns 가 예외로 실패하고 표면 사전도 비면 종결 판정 없이 재시도 가능한 실패다."""
        from src.gold.absa_analyzer import Okt
        from src.utils.logger import get_logger

        class _NounsFailingOkt(Okt):
            # 결함 주입: 형태소 분석기는 실제 Okt 이고 nouns 호출만 실패시킨다
            def nouns(self, phrase):
                raise RuntimeError("injected Okt.nouns failure")

        analyzer = GoldABSAAnalyzer.__new__(GoldABSAAnalyzer)
        analyzer.logger = get_logger(__name__)
        analyzer.db_connector = None
        analyzer._okt = _NounsFailingOkt()
        review_id = _seed_cleaned_review(test_db_session, refined_text)

        assert analyzer.process(test_db_session, review_id) is False
        assert _aspect_count(test_db_session, review_id) == 0


@pytest.mark.requires_db
def test_process_batch_records_skipped_reviews(
    test_db_engine, test_db_url, test_db_schema, okt_analyzer, monkeypatch
):
    """standalone 배치(features 단계)도 분석 제외 리뷰를 SKIPPED 로 남기고 retry_count 를 유지한다."""
    from sqlalchemy import text as sql

    monkeypatch.setenv("DATABASE_URL", test_db_url)
    app_id = uuid7()
    seeded = {}
    try:
        with test_db_engine.begin() as connection:
            connection.execute(
                sql(
                    "INSERT INTO apps (app_id, platform_app_id, platform_type, name) "
                    "VALUES (:app_id, :pid, 'APPSTORE', 'ABSA Batch Skip Test App')"
                ),
                {"app_id": app_id, "pid": f"absa.batch.skip.{app_id}"},
            )
            for refined_text, reason in (("", "skip:empty"), ("Not working at all", "skip:non_korean")):
                review_id = uuid7()
                prid = f"absa-batch-skip-{review_id}"
                connection.execute(
                    sql(
                        "INSERT INTO review_master_index (review_id, app_id, platform_review_id, "
                        "platform_type, processing_status, retry_count) "
                        "VALUES (:review_id, :app_id, :prid, 'APPSTORE', 'CLEANED', 1)"
                    ),
                    {"review_id": review_id, "app_id": app_id, "prid": prid},
                )
                connection.execute(
                    sql(
                        "INSERT INTO reviews_preprocessed (review_id, platform_review_id, refined_text) "
                        "VALUES (:review_id, :prid, :refined_text)"
                    ),
                    {"review_id": review_id, "prid": prid, "refined_text": refined_text},
                )
                seeded[review_id] = reason

        GoldABSAAnalyzer().process_batch(batch_size=10)

        with test_db_engine.connect() as connection:
            rows = connection.execute(
                sql(
                    "SELECT review_id, processing_status, error_message, retry_count "
                    "FROM review_master_index WHERE app_id = :app_id"
                ),
                {"app_id": app_id},
            ).all()
        assert {row.review_id: row.error_message for row in rows} == seeded
        assert {(row.processing_status, row.retry_count) for row in rows} == {("SKIPPED", 1)}
    finally:
        with test_db_engine.begin() as connection:
            connection.execute(
                sql(
                    "DELETE FROM reviews_preprocessed WHERE review_id IN "
                    "(SELECT review_id FROM review_master_index WHERE app_id = :app_id)"
                ),
                {"app_id": app_id},
            )
            connection.execute(
                sql("DELETE FROM review_master_index WHERE app_id = :app_id"), {"app_id": app_id}
            )
            connection.execute(sql("DELETE FROM apps WHERE app_id = :app_id"), {"app_id": app_id})
