"""
리뷰 임베딩 데이터 모델 (Silver Layer)
"""
from sqlalchemy import Column, String, DateTime, BigInteger, ForeignKey
from sqlalchemy.sql import func
from .base import Base

# pgvector 지원을 위한 import
try:
    from pgvector.sqlalchemy import Vector
    PGVECTOR_AVAILABLE = True
except ImportError:
    PGVECTOR_AVAILABLE = False
    # Fallback: pgvector 없으면 에러 발생하도록
    Vector = None


class ReviewEmbedding(Base):
    """리뷰 임베딩 데이터 모델 (review_embeddings 테이블)"""
    __tablename__ = 'review_embeddings'

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    app_review_id = Column(BigInteger, ForeignKey('app_reviews.id'), nullable=False)
    source_content_type = Column(String, nullable=False)  # 'raw', 'preprocessed', 'features'
    model_name = Column(String, nullable=True)
    # OpenAI text-embedding-3-small 기본 차원 1536
    vector = Column(Vector(1536) if Vector else Vector, nullable=True)  # pgvector VECTOR type
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now())
