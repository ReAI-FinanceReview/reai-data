"""중앙 리뷰 인덱스 모델

This module defines the ReviewMasterIndex model, which serves as the central hub
for all review-related data across the pipeline (Bronze → Silver → Gold).
"""

from sqlalchemy import Column, Text, DateTime, Boolean, Integer, Enum as SQLEnum, ForeignKey
from sqlalchemy.dialects.postgresql import UUID

from .base import Base
from .enums import PlatformType, ProcessingStatusType


class ReviewMasterIndex(Base):
    """중앙 리뷰 인덱스 테이블 (DB)

    모든 리뷰 데이터의 중앙 허브 역할을 하는 테이블입니다.
    Bronze(Parquet) → Silver(분석) → Gold(배정) 파이프라인의 핵심 연결고리입니다.
    """
    __tablename__ = 'review_master_index'

    review_id = Column(
        UUID(as_uuid=True),
        primary_key=True,
        comment='리뷰 고유 ID (UUID v7)'
    )
    app_id = Column(
        UUID(as_uuid=True),
        ForeignKey('apps.app_id'),
        nullable=False,
        comment='앱 ID (FK to apps)'
    )
    service_id = Column(
        UUID(as_uuid=True),
        comment='서비스 ID (denormalized from app_metadata)'
    )
    platform_review_id = Column(
        Text,
        nullable=False,
        unique=True,
        comment='플랫폼 원본 리뷰 ID (중복 방지용)'
    )
    platform_type = Column(
        SQLEnum(PlatformType, name='platform_type', create_type=False),
        comment='플랫폼 타입 (APPSTORE, PLAYSTORE)'
    )
    review_created_at = Column(
        DateTime(timezone=True),
        comment='리뷰 작성 시각'
    )
    ingested_at = Column(
        DateTime(timezone=True),
        comment='리뷰 수집 시각'
    )
    processing_status = Column(
        SQLEnum(ProcessingStatusType, name='processing_status_type', create_type=False),
        comment='처리 상태 (RAW, CLEANED, ANALYZED, FAILED)'
    )
    parquet_written_at = Column(
        DateTime(timezone=True),
        comment='Parquet 쓰기 성공 시각 (Phase 1 of 2-phase commit)'
    )
    storage_path = Column(
        Text,
        comment='Parquet 파일 경로 (MinIO 또는 로컬)'
    )
    error_message = Column(
        Text,
        comment='실패 사유 (Parquet write / DB commit 에러 메시지)'
    )
    retry_count = Column(
        Integer,
        default=0,
        comment='재시도 횟수 (최대 3회, 초과 시 DLQ)'
    )
    is_active = Column(Boolean, comment='활성 여부')
    is_reply = Column(Boolean, comment='답글 여부')

    def __repr__(self):
        return (
            f"<ReviewMasterIndex(review_id={self.review_id}, "
            f"platform_review_id='{self.platform_review_id}', "
            f"status={self.processing_status})>"
        )
