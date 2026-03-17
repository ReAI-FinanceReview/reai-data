"""Gold Layer - ABSA (Aspect-Based Sentiment Analysis) Engine

reviews_preprocessed.refined_text에서 키워드/감성/카테고리를 추출하여
review_aspects에 적재.

감성 공식:
    S_final = S_base × W_adv
    negation 감지 시: S_final = 1.0 - S_final
    범위: 0.0 (매우 부정) ~ 1.0 (매우 긍정)

카테고리 매핑 (우선순위):
    1차: 규칙 기반 키워드 사전 → CategoryType
    2차: review_embeddings 벡터 vs 앵커 벡터 코사인 유사도 (≥ 0.8)
    미분류: None

Usage (standalone):
    analyzer = GoldABSAAnalyzer()
    analyzer.process_batch(batch_size=100)

Usage (via orchestrator):
    success = analyzer.process(session, review_id)
"""

from __future__ import annotations

import math
from typing import Dict, List, Optional, Tuple
from uuid import UUID

try:
    from konlpy.tag import Okt
    KONLPY_AVAILABLE = True
except ImportError:
    KONLPY_AVAILABLE = False
    Okt = None  # type: ignore

from src.models.enums import CategoryType, ProcessingStatusType
from src.models.review_aspects import ReviewAspect
from src.models.review_preprocessed import ReviewPreprocessed
from src.utils.db_connector import DatabaseConnector
from src.utils.logger import get_logger


# ----------------------------------------------------------------
# 금융 감성 사전  {단어: 기본점수(0.0~1.0)}
# 0.5 = 중립, >0.5 = 긍정, <0.5 = 부정
# ----------------------------------------------------------------
_SENTIMENT_DICT: Dict[str, float] = {
    # 긍정
    "편리": 0.85, "편의": 0.80, "빠르": 0.80, "간편": 0.85,
    "좋": 0.80, "훌륭": 0.90, "만족": 0.85, "추천": 0.85,
    "쉽": 0.80, "깔끔": 0.75, "안정": 0.80, "친절": 0.80,
    "개선": 0.70, "업데이트": 0.65, "해결": 0.75,
    # 부정
    "불편": 0.20, "느리": 0.20, "오류": 0.10, "버그": 0.10,
    "에러": 0.10, "강제종료": 0.05, "팅김": 0.10, "먹통": 0.05,
    "최악": 0.05, "짜증": 0.10, "불만": 0.15, "실망": 0.15,
    "어렵": 0.25, "복잡": 0.25, "답답": 0.20, "느림": 0.20,
}

# 부사 가중치 {부사: 배율}
_ADV_WEIGHTS: Dict[str, float] = {
    "매우": 1.3, "정말": 1.3, "너무": 1.2, "진짜": 1.2,
    "엄청": 1.3, "완전": 1.2, "굉장히": 1.3, "극도로": 1.4,
    "좀": 0.8, "약간": 0.8, "조금": 0.8, "살짝": 0.7,
}

# 부정어
_NEGATION_WORDS = {"안", "못", "없", "아니", "않"}

# ----------------------------------------------------------------
# 카테고리 키워드 사전  {CategoryType: [키워드...]}
# ----------------------------------------------------------------
_CATEGORY_KEYWORDS: Dict[CategoryType, List[str]] = {
    CategoryType.USABILITY: [
        "사용", "편리", "편의", "인터페이스", "UI", "UX", "화면", "메뉴",
        "기능", "조작", "접근", "쉽", "어렵", "복잡", "간편",
    ],
    CategoryType.STABILITY: [
        "오류", "버그", "에러", "강제종료", "팅김", "먹통", "충돌",
        "안정", "불안정", "다운", "멈춤", "튕김",
    ],
    CategoryType.DESIGN: [
        "디자인", "UI", "화면", "레이아웃", "색상", "폰트", "글자",
        "깔끔", "예쁘", "구성", "아이콘",
    ],
    CategoryType.CUSTOMER_SUPPORT: [
        "고객", "상담", "CS", "고객센터", "응대", "문의", "지원",
        "도움", "답변", "서비스", "직원", "친절",
    ],
    CategoryType.SPEED: [
        "속도", "빠르", "느리", "로딩", "반응", "렉", "지연",
        "빠름", "느림", "쾌적", "버벅",
    ],
}

# 역방향 조회: 키워드 → CategoryType (1차 매핑용)
_KEYWORD_TO_CATEGORY: Dict[str, CategoryType] = {
    kw: cat
    for cat, keywords in _CATEGORY_KEYWORDS.items()
    for kw in keywords
}


class GoldABSAAnalyzer:
    """reviews_preprocessed → review_aspects 적재 (Gold Layer).

    Orchestrator 단일 건 처리와 standalone 배치 처리 모두 지원.
    """

    def __init__(self, config_path: str = "config/crawler_config.yml"):
        self.logger = get_logger(__name__)
        self.db_connector = DatabaseConnector(config_path)
        self._okt = self._init_okt()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def process(self, session, review_id: UUID) -> bool:
        """단일 review_id에 대해 ABSA 수행 후 DB 적재.

        Orchestrator에서 호출. 세션 관리는 호출자 책임.

        Returns:
            True: 성공(신규 적재 또는 이미 존재)
            False: 실패
        """
        if self._is_already_analyzed(session, review_id):
            return True

        preprocessed = session.get(ReviewPreprocessed, review_id)
        if preprocessed is None or not preprocessed.refined_text:
            self.logger.warning(f"[{review_id}] No refined_text — skip")
            return True

        aspects = self._analyze(session, review_id, preprocessed.refined_text)
        session.add_all(aspects)
        return True

    def process_batch(self, batch_size: int = 100, limit: Optional[int] = None) -> int:
        """CLEANED 상태이면서 review_aspects 미생성 리뷰를 배치 처리.

        Returns:
            성공적으로 처리된 리뷰 수
        """
        session = self.db_connector.get_session()
        try:
            review_ids = self._fetch_pending_review_ids(session, limit)
            if not review_ids:
                self.logger.info("No reviews pending ABSA")
                return 0

            self.logger.info(f"Running ABSA for {len(review_ids)} reviews")
            success_count = 0

            for i in range(0, len(review_ids), batch_size):
                chunk = review_ids[i:i + batch_size]
                for review_id in chunk:
                    if self.process(session, review_id):
                        success_count += 1
                    else:
                        self.logger.warning(f"[{review_id}] ABSA failed — skipping")

                session.commit()
                self.logger.info(
                    f"Progress: {min(i + batch_size, len(review_ids))}/{len(review_ids)}"
                )

            self.logger.info(f"ABSA complete: {success_count}/{len(review_ids)} succeeded")
            return success_count

        except Exception:
            session.rollback()
            self.logger.exception("Batch ABSA failed")
            raise
        finally:
            session.close()

    # ------------------------------------------------------------------
    # Analysis core
    # ------------------------------------------------------------------

    def _analyze(
        self, session, review_id: UUID, text: str
    ) -> List[ReviewAspect]:
        """텍스트에서 aspect 목록을 추출하여 ReviewAspect 리스트 반환."""
        keywords = self._extract_keywords(text)
        if not keywords:
            return []

        has_negation = self._has_negation(text)
        adv_weight = self._get_adv_weight(text)

        aspects: List[ReviewAspect] = []
        for keyword in keywords:
            s_base = _SENTIMENT_DICT.get(keyword, 0.5)
            s_final = s_base * adv_weight
            if has_negation:
                s_final = 1.0 - s_final
            s_final = max(0.0, min(1.0, s_final))  # clamp

            category = self._map_category(session, review_id, keyword)

            aspects.append(ReviewAspect(
                review_id=review_id,
                keyword=keyword,
                sentiment_score=round(s_final, 4),
                category=category.value if category else None,
            ))

        return aspects

    def _extract_keywords(self, text: str) -> List[str]:
        """KoNLPy Okt로 명사 추출. unavailable 시 규칙 기반 fallback."""
        if self._okt:
            try:
                nouns = self._okt.nouns(text)
                # 2글자 이상 + 감성/카테고리 사전에 있는 단어 우선
                filtered = [n for n in nouns if len(n) >= 2]
                return list(dict.fromkeys(filtered))[:20]  # 중복 제거, 최대 20개
            except Exception as e:
                self.logger.warning(f"Okt.nouns failed: {e} — fallback to dict match")

        # Fallback: 감성 사전 + 카테고리 키워드 사전 직접 매칭
        found = [w for w in _SENTIMENT_DICT if w in text]
        found += [w for w in _KEYWORD_TO_CATEGORY if w in text and w not in found]
        return found[:20]

    def _has_negation(self, text: str) -> bool:
        """텍스트에 부정어 포함 여부."""
        tokens = text.split()
        return any(neg in token for token in tokens for neg in _NEGATION_WORDS)

    def _get_adv_weight(self, text: str) -> float:
        """텍스트에서 가장 강한 부사 가중치 반환. 없으면 1.0."""
        weight = 1.0
        for adv, w in _ADV_WEIGHTS.items():
            if adv in text:
                # 강도를 높이는 방향으로 가장 큰 편차를 선택
                if abs(w - 1.0) > abs(weight - 1.0):
                    weight = w
        return weight

    def _map_category(
        self, session, review_id: UUID, keyword: str
    ) -> Optional[CategoryType]:
        """키워드 → CategoryType 매핑 (1차: 규칙, 2차: 벡터 유사도)."""
        # 1차: 규칙 기반
        for cat, keywords in _CATEGORY_KEYWORDS.items():
            if any(keyword.startswith(k) or k.startswith(keyword) for k in keywords):
                return cat

        # 2차: 벡터 유사도 (review_embeddings가 있을 때만)
        return self._map_category_by_vector(session, review_id)

    def _map_category_by_vector(
        self, session, review_id: UUID
    ) -> Optional[CategoryType]:
        """pgvector 코사인 유사도로 카테고리 매핑.

        review_embeddings 벡터와 사전 정의된 앵커 벡터를 비교.
        유사도 < 0.8이거나 임베딩 없으면 None 반환.
        """
        from src.models.review_embedding import ReviewEmbedding

        embedding = session.get(ReviewEmbedding, review_id)
        if embedding is None or embedding.vector is None:
            return None

        try:
            review_vec = list(embedding.vector)
            best_cat: Optional[CategoryType] = None
            best_sim = 0.0

            for cat, anchor in _ANCHOR_VECTORS.items():
                sim = _cosine_similarity(review_vec, anchor)
                if sim > best_sim:
                    best_sim = sim
                    best_cat = cat

            return best_cat if best_sim >= 0.8 else None
        except Exception as e:
            self.logger.warning(f"Vector category mapping failed: {e}")
            return None

    # ------------------------------------------------------------------
    # DB helpers
    # ------------------------------------------------------------------

    def _is_already_analyzed(self, session, review_id: UUID) -> bool:
        """review_aspects에 해당 review_id가 이미 존재하는지 확인."""
        return (
            session.query(ReviewAspect.aspect_id)
            .filter(ReviewAspect.review_id == review_id)
            .first()
        ) is not None

    def _fetch_pending_review_ids(
        self, session, limit: Optional[int]
    ) -> List[UUID]:
        """ABSA 미처리 review_id 조회 (CLEANED 상태 기준)."""
        from sqlalchemy import not_, exists
        from src.models.review_master_index import ReviewMasterIndex

        query = (
            session.query(ReviewPreprocessed.review_id)
            .join(
                ReviewMasterIndex,
                ReviewMasterIndex.review_id == ReviewPreprocessed.review_id,
            )
            .filter(ReviewMasterIndex.processing_status == ProcessingStatusType.CLEANED)
            .filter(
                not_(
                    exists().where(
                        ReviewAspect.review_id == ReviewPreprocessed.review_id
                    )
                )
            )
        )
        if limit:
            query = query.limit(limit)
        return [row.review_id for row in query.all()]

    # ------------------------------------------------------------------
    # Init helpers
    # ------------------------------------------------------------------

    def _init_okt(self):
        if not KONLPY_AVAILABLE:
            self.logger.warning(
                "konlpy not available — keyword extraction will use dict-match fallback"
            )
            return None
        try:
            return Okt()
        except Exception as e:
            self.logger.warning(f"Okt init failed: {e} — using dict-match fallback")
            return None


# ----------------------------------------------------------------
# 카테고리 앵커 벡터 (1536-dim, text-embedding-3-small 기준)
# 런타임에 OpenAI API로 생성하는 대신, 대표 문장의 평균 벡터를
# 미리 계산하여 상수로 저장. 여기서는 uniform placeholder 사용.
# 실제 운영 시 scripts/generate_anchor_vectors.py로 생성 후 교체.
# ----------------------------------------------------------------
def _make_placeholder_anchor(seed: int, dim: int = 1536) -> List[float]:
    """테스트용 단위 벡터 생성 (실제 임베딩으로 교체 필요)."""
    import hashlib
    h = int(hashlib.md5(str(seed).encode()).hexdigest(), 16)
    vec = [(((h >> i) & 0xFF) / 255.0 - 0.5) for i in range(dim)]
    norm = math.sqrt(sum(v * v for v in vec)) or 1.0
    return [v / norm for v in vec]


_ANCHOR_VECTORS: Dict[CategoryType, List[float]] = {
    CategoryType.USABILITY:        _make_placeholder_anchor(1),
    CategoryType.STABILITY:        _make_placeholder_anchor(2),
    CategoryType.DESIGN:           _make_placeholder_anchor(3),
    CategoryType.CUSTOMER_SUPPORT: _make_placeholder_anchor(4),
    CategoryType.SPEED:            _make_placeholder_anchor(5),
}


def _cosine_similarity(a: List[float], b: List[float]) -> float:
    dot = sum(x * y for x, y in zip(a, b))
    norm_a = math.sqrt(sum(x * x for x in a)) or 1.0
    norm_b = math.sqrt(sum(x * x for x in b)) or 1.0
    return dot / (norm_a * norm_b)
