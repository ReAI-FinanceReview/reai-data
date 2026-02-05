"""전처리된 리뷰 데이터 모델 (Silver - NAS Parquet)

This module defines the ReviewPreprocessed model for cleaned review text.
Note: This table is stored as Parquet on NAS, not actively queried in DB.
"""

from sqlalchemy import Column, DateTime, Text
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.sql import func

from .base import Base


class ReviewPreprocessed(Base):
    """전처리된 리뷰 데이터 테이블 (Silver - NAS Parquet)

    텍스트 정제가 완료된 리뷰 데이터를 저장합니다.
    실제로는 NAS의 Parquet 파일로 저장되며, DB에는 메타데이터만 유지됩니다.
    """
    __tablename__ = 'reviews_preprocessed'

    review_id = Column(
        UUID(as_uuid=True),
        primary_key=True,
        comment='리뷰 고유 ID (UUID v7)'
    )
    platform_review_id = Column(
        Text,
        nullable=False,
        unique=True,
        comment='플랫폼 원본 리뷰 ID (중복 방지)'
    )
    refined_text = Column(Text, comment='정제된 리뷰 텍스트')
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
            f"<ReviewPreprocessed(review_id={self.review_id}, "
            f"platform_review_id='{self.platform_review_id}')>"
        )
