"""리뷰 임베딩 데이터 모델 (Silver - DB)

This module defines the ReviewEmbedding model for vector embeddings of reviews.
"""

from sqlalchemy import Column, String, DateTime, ForeignKey
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.sql import func

from .base import Base

# pgvector 지원을 위한 import
try:
    from pgvector.sqlalchemy import Vector
    PGVECTOR_AVAILABLE = True
except ImportError:
    PGVECTOR_AVAILABLE = False
    Vector = None


class ReviewEmbedding(Base):
    """리뷰 임베딩 데이터 테이블 (Silver - DB)

    리뷰의 벡터 임베딩을 저장합니다 (pgvector 사용).
    임베딩은 DB에 직접 저장되어 벡터 유사도 검색에 활용됩니다.
    """
    __tablename__ = 'review_embeddings'

    review_id = Column(
        UUID(as_uuid=True),
        ForeignKey('review_master_index.review_id'),
        primary_key=True,
        comment='리뷰 ID (FK to review_master_index, PK)'
    )
    source_content_type = Column(
        String,
        nullable=False,
        comment='소스 타입 (raw, preprocessed, features)'
    )
    model_name = Column(String, comment='임베딩 모델 이름')
    # OpenAI text-embedding-3-small 기본 차원 1536
    vector = Column(
        Vector(1536) if Vector else None,
        comment='임베딩 벡터 (pgvector)'
    )
    created_at = Column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        comment='생성 시각'
    )
    updated_at = Column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
        comment='수정 시각'
    )

    def __repr__(self):
        return (
            f"<ReviewEmbedding(review_id={self.review_id}, "
            f"model_name='{self.model_name}', source_type='{self.source_content_type}')>"
        )
