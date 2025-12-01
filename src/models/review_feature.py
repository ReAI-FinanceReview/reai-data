"""
리뷰 특성 데이터 모델 (Silver Layer)
Matches DBinit.sql reviews_features table schema
"""
from sqlalchemy import Column, String, BigInteger, Float, ForeignKey, ARRAY, Enum as SQLEnum, DateTime
from sqlalchemy.sql import func
from .base import Base
import enum


class SentimentType(enum.Enum):
    """감성 분석 결과 ENUM (matches DB sentiment_type)"""
    POSITIVE = "POSITIVE"
    NEGATIVE = "NEGATIVE"
    NEUTRAL = "NEUTRAL"


class ReviewFeature(Base):
    """리뷰 특성 데이터 모델 (reviews_features 테이블)"""
    __tablename__ = 'reviews_features'

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    review_preprocessed_id = Column(BigInteger, ForeignKey('reviews_preprocessed.id'), nullable=False, unique=True)
    sentiment = Column(SQLEnum(SentimentType, name='sentiment_type', create_type=False), nullable=True)
    sentiment_score = Column(Float, nullable=True)
    keywords = Column(ARRAY(String), nullable=True)
    topics = Column(ARRAY(String), nullable=True)
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now())
