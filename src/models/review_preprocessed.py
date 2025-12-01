"""
전처리된 리뷰 데이터 모델 (Silver Layer)
"""
from sqlalchemy import Column, DateTime, Text, BigInteger, ForeignKey
from sqlalchemy.sql import func
from .base import Base


class ReviewPreprocessed(Base):
    """전처리된 리뷰 데이터 모델 (reviews_preprocessed 테이블)"""
    __tablename__ = 'reviews_preprocessed'

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    app_review_id = Column(BigInteger, ForeignKey('app_reviews.id'), nullable=False, unique=True)
    refined_text = Column(Text, nullable=True)
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now())
