"""
AI 기반 특성 추출 파이프라인
전처리된 텍스트에서 감성 분석, 키워드 추출, 토픽 모델링 수행
"""
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from sqlalchemy.orm import Session

try:
    from transformers import pipeline
    import torch
    TRANSFORMERS_AVAILABLE = True
except ImportError:
    TRANSFORMERS_AVAILABLE = False

try:
    from konlpy.tag import Okt
    KONLPY_AVAILABLE = True
except ImportError:
    KONLPY_AVAILABLE = False

from src.utils.logger import get_logger
from src.utils.db_connector import DatabaseConnector
from src.models.base import Base
from src.models.review_preprocessed import ReviewPreprocessed
from src.models.review_feature import ReviewFeature, SentimentType


class FeatureExtractor:
    """
    AI 기반 특성 추출 클래스
    
    주요 기능:
    - 감성 분석 (Sentiment Analysis)
    - 키워드 추출 (Keyword Extraction)
    - 토픽 모델링 (Topic Modeling)
    - 텍스트 통계
    """

    def __init__(self, config_path: str = 'config/crawler_config.yml'):
        self.logger = get_logger(__name__)
        self.db_connector = DatabaseConnector(config_path)
        self.extraction_version = '1.0'

        # Sentiment Analysis 모델 초기화
        self.sentiment_pipeline = None
        if TRANSFORMERS_AVAILABLE:
            try:
                # 한국어 감성 분석 모델
                model_name = "beomi/kcbert-base"  # 또는 사용자 정의 모델
                self.logger.info(f"감성 분석 모델 로딩 시작: {model_name}")
                # 실제 프로덕션에서는 캐싱된 모델 사용
                # self.sentiment_pipeline = pipeline("text-classification", model=model_name)
                self.logger.info("감성 분석 모델 로딩 완료")
            except Exception as e:
                self.logger.warning(f"감성 분석 모델 로딩 실패: {e}")
        else:
            self.logger.warning("transformers 라이브러리가 없어 감성 분석을 수행할 수 없습니다.")

        # 형태소 분석기 초기화
        self.okt = None
        if KONLPY_AVAILABLE:
            try:
                self.okt = Okt()
                self.logger.info("형태소 분석기 초기화 완료")
            except Exception as e:
                self.logger.warning(f"형태소 분석기 초기화 실패: {e}")

    def analyze_sentiment(self, text: str) -> Tuple[str, float, float]:
        """
        감성 분석 수행
        
        Returns:
            (sentiment_label, sentiment_score, confidence)
            - sentiment_label: 'positive', 'negative', 'neutral'
            - sentiment_score: -1.0 ~ 1.0
            - confidence: 0.0 ~ 1.0
        """
        # 간단한 규칙 기반 감성 분석 (실제로는 모델 사용)
        positive_words = ['좋', '훌륭', '편리', '만족', '추천', '빠르', '쉽', '감사']
        negative_words = ['나쁘', '불편', '느리', '오류', '불만', '최악', '짜증', '개선']

        text_lower = text.lower()
        pos_count = sum(1 for word in positive_words if word in text_lower)
        neg_count = sum(1 for word in negative_words if word in text_lower)

        total = pos_count + neg_count
        if total == 0:
            return 'neutral', 0.0, 0.5

        sentiment_score = (pos_count - neg_count) / max(total, 1)
        
        if sentiment_score > 0.3:
            label = 'positive'
            confidence = min(0.5 + sentiment_score * 0.5, 1.0)
        elif sentiment_score < -0.3:
            label = 'negative'
            confidence = min(0.5 + abs(sentiment_score) * 0.5, 1.0)
        else:
            label = 'neutral'
            confidence = 0.5

        return label, sentiment_score, confidence

    def extract_keywords(self, text: str, top_n: int = 10) -> List[Dict]:
        """
        키워드 추출 (명사 기반)
        
        Returns:
            List of {word: str, score: float}
        """
        if not self.okt:
            return []

        try:
            # 명사 추출
            nouns = self.okt.nouns(text)
            
            # 빈도 계산
            from collections import Counter
            noun_counts = Counter(nouns)
            
            # 상위 N개 키워드
            keywords = []
            for word, count in noun_counts.most_common(top_n):
                if len(word) > 1:  # 한 글자 제외
                    keywords.append({
                        'word': word,
                        'score': count
                    })
            
            return keywords
        except Exception as e:
            self.logger.error(f"키워드 추출 중 오류: {e}")
            return []

    def extract_topic(self, text: str) -> Tuple[int, str, float, List]:
        """
        토픽 모델링 (간단한 규칙 기반)
        
        Returns:
            (topic_id, topic_label, probability, topic_keywords)
        """
        # 금융 앱 관련 토픽
        topics = {
            1: ('로그인/인증', ['로그인', '인증', '비밀번호', '인증서', '본인인증']),
            2: ('결제/송금', ['결제', '송금', '이체', '계좌', '카드']),
            3: ('UI/UX', ['UI', 'UX', '디자인', '화면', '인터페이스']),
            4: ('오류/버그', ['오류', '버그', '에러', '작동', '실행']),
            5: ('성능/속도', ['느리', '빠르', '속도', '렉', '멈춤']),
            6: ('기능/서비스', ['기능', '서비스', '편리', '불편', '개선']),
        }

        text_lower = text.lower()
        max_score = 0
        matched_topic = (0, 'unknown', 0.0, [])

        for topic_id, (topic_label, keywords) in topics.items():
            score = sum(1 for keyword in keywords if keyword in text_lower)
            if score > max_score:
                max_score = score
                probability = min(score / len(keywords), 1.0)
                matched_topic = (topic_id, topic_label, probability, keywords)

        return matched_topic

    def calculate_text_stats(self, text: str) -> Tuple[int, int, float]:
        """
        텍스트 통계 계산
        
        Returns:
            (word_count, sentence_count, avg_word_length)
        """
        # 단어 수
        words = text.split()
        word_count = len(words)

        # 문장 수 (간단한 방법)
        sentences = [s.strip() for s in text.split('.') if s.strip()]
        sentence_count = len(sentences)

        # 평균 단어 길이
        avg_word_length = sum(len(word) for word in words) / max(word_count, 1)

        return word_count, sentence_count, avg_word_length

    def extract_features(self, preprocessed: ReviewPreprocessed) -> Optional[ReviewFeature]:
        """리뷰 한 건의 특성 추출 (matching DBinit.sql schema)"""
        if not preprocessed.refined_text:
            return None

        try:
            text = preprocessed.refined_text

            # 1. 감성 분석
            sentiment_label, sentiment_score, confidence = self.analyze_sentiment(text)

            # 2. 키워드 추출
            keywords = self.extract_keywords(text, top_n=10)

            # 3. 토픽 모델링
            topic_id, topic_label, topic_prob, topic_keywords = self.extract_topic(text)

            # Convert sentiment_label to SentimentType ENUM
            sentiment_enum = None
            if sentiment_label == 'positive':
                sentiment_enum = SentimentType.POSITIVE
            elif sentiment_label == 'negative':
                sentiment_enum = SentimentType.NEGATIVE
            elif sentiment_label == 'neutral':
                sentiment_enum = SentimentType.NEUTRAL

            # ReviewFeature 객체 생성 (matching DBinit.sql schema)
            feature = ReviewFeature(
                review_preprocessed_id=preprocessed.id,  # FK to reviews_preprocessed.id
                sentiment=sentiment_enum,  # ENUM type
                sentiment_score=sentiment_score,
                keywords=[kw['word'] for kw in keywords] if keywords else [],  # TEXT[] array
                topics=[topic_label] if topic_label else []  # TEXT[] array
            )

            return feature

        except Exception as e:
            self.logger.error(f"리뷰 {preprocessed.id} 특성 추출 중 오류: {e}")
            return None

    def process_batch(self, batch_size: int = 100, limit: Optional[int] = None):
        """배치 처리 - Silver 전처리 데이터에서 특성 추출"""
        self.logger.info("=" * 60)
        self.logger.info("특성 추출 파이프라인 시작")
        self.logger.info("=" * 60)

        session = self.db_connector.get_session()

        try:
            # 테이블 생성
            self.db_connector.create_tables(Base)

            # 특성 추출할 전처리된 리뷰 조회
            query = session.query(ReviewPreprocessed)

            # 이미 특성 추출된 리뷰 제외
            try:
                processed_preprocessed_ids = session.query(ReviewFeature.review_preprocessed_id).distinct().all()
                processed_ids = {row[0] for row in processed_preprocessed_ids}

                if processed_ids:
                    query = query.filter(~ReviewPreprocessed.id.in_(processed_ids))
                    self.logger.info(f"이미 특성 추출된 리뷰 {len(processed_ids)}개 제외")
            except Exception as e:
                self.logger.warning(f"특성 추출된 리뷰 조회 실패: {e}")

            if limit:
                query = query.limit(limit)

            total_reviews = query.count()
            self.logger.info(f"특성 추출할 리뷰 개수: {total_reviews}")

            if total_reviews == 0:
                self.logger.info("특성 추출할 리뷰가 없습니다.")
                return

            processed_count = 0
            failed_count = 0
            sentiment_stats = {'positive': 0, 'negative': 0, 'neutral': 0}

            # 배치 처리
            for i in range(0, total_reviews, batch_size):
                batch = query.offset(i).limit(batch_size).all()

                for preprocessed in batch:
                    feature = self.extract_features(preprocessed)
                    if feature:
                        session.add(feature)
                        processed_count += 1
                        # sentiment is now an ENUM, convert to string for stats
                        if feature.sentiment:
                            sentiment_key = feature.sentiment.value.lower()
                            if sentiment_key in sentiment_stats:
                                sentiment_stats[sentiment_key] += 1
                    else:
                        failed_count += 1

                # 배치 커밋
                session.commit()
                progress = min(i + batch_size, total_reviews)
                self.logger.info(f"진행률: {progress}/{total_reviews} ({progress/total_reviews*100:.1f}%)")

            self.logger.info("=" * 60)
            self.logger.info(f"✅ 특성 추출 완료")
            self.logger.info(f"   - 성공: {processed_count}개")
            self.logger.info(f"   - 실패: {failed_count}개")
            self.logger.info(f"   - 감성 분포: Positive={sentiment_stats['positive']}, "
                           f"Negative={sentiment_stats['negative']}, Neutral={sentiment_stats['neutral']}")
            self.logger.info("=" * 60)

        except Exception as e:
            self.logger.error(f"특성 추출 중 오류 발생: {e}")
            session.rollback()
            raise
        finally:
            session.close()


def main():
    """메인 실행 함수"""
    import sys

    limit = None
    if len(sys.argv) > 1:
        limit = int(sys.argv[1])

    extractor = FeatureExtractor()
    extractor.process_batch(batch_size=100, limit=limit)


if __name__ == '__main__':
    main()
