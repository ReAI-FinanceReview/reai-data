"""리뷰 원본 데이터 모델 (Bronze - NAS Parquet)

This module defines the Review model representing raw review data.
Note: This table is stored as Parquet on NAS, not actively queried in DB.
"""

import enum

from sqlalchemy import Column, String, SmallInteger, Text, DateTime, Boolean, Enum as SQLEnum
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.sql import func

from .base import Base


class PlatformType(enum.Enum):
    """플랫폼 타입 ENUM"""
    APPSTORE = "APPSTORE"
    PLAYSTORE = "PLAYSTORE"


class Review(Base):
    """리뷰 원본 데이터 테이블 (Bronze - NAS Parquet)

    앱 리뷰의 원본 데이터를 저장합니다.
    실제로는 NAS의 Parquet 파일로 저장되며, DB에는 메타데이터만 유지됩니다.
    """
    __tablename__ = 'app_reviews'

    # Note: This table is stored as Parquet on NAS, not actively queried in DB
    review_id = Column(
        UUID(as_uuid=True),
        primary_key=True,
        comment='리뷰 고유 ID (UUID v7)'
    )
    app_id = Column(
        UUID(as_uuid=True),
        nullable=False,
        comment='앱 ID (references Parquet, no FK)'
    )
    platform = Column(
        SQLEnum(PlatformType, name='platform_type', create_type=False),
        nullable=False,
        comment='플랫폼 타입 (APPSTORE, PLAYSTORE)'
    )
    country_code = Column(
        String,
        nullable=False,
        server_default='kr',
        comment='국가 코드'
    )
    platform_review_id = Column(
        String,
        nullable=False,
        comment='플랫폼 원본 리뷰 ID'
    )
    reviewer_name = Column(String, comment='리뷰어 이름')
    review_text = Column(Text, nullable=False, comment='리뷰 본문')
    rating = Column(SmallInteger, nullable=False, comment='평점')
    app_version = Column(String, comment='앱 버전')
    reviewed_at = Column(
        DateTime(timezone=True),
        nullable=False,
        comment='리뷰 작성 시각'
    )
    created_at = Column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        comment='수집 시각'
    )
    updated_at = Column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
        comment='수정 시각'
    )
    is_reply = Column(Boolean, comment='답글 여부')
    reply_comment = Column(Text, comment='개발자 답글')

    def __repr__(self):
        return (
            f"<Review(review_id={self.review_id}, app_id={self.app_id}, "
            f"platform={self.platform}, rating={self.rating})>"
        )
