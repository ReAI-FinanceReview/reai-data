"""Ingestion Batch 모델

Parquet 배치 적재 관리 (DLQ) - crawl 단계에서 생성, load 단계에서 소비.
"""

from sqlalchemy import Column, Text, Integer, Boolean, Enum as SQLEnum
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy import DateTime

from .base import Base
from .enums import PlatformType, IngestionBatchStatusType


class IngestionBatch(Base):
    """Parquet 배치 적재 상태 테이블.

    crawl 단계에서 Parquet 파일 쓰기 성공 시 PENDING 레코드를 생성하고,
    load 단계(BatchLoader)에서 PENDING/FAILED 배치를 조회하여 DB에 적재합니다.
    """
    __tablename__ = 'ingestion_batch'

    batch_id = Column(
        UUID(as_uuid=True),
        primary_key=True,
        comment='배치 고유 ID (UUID)'
    )
    source_type = Column(
        SQLEnum(PlatformType, name='platform_type', create_type=False),
        nullable=False,
        comment='플랫폼 타입 (APPSTORE / PLAYSTORE)'
    )
    platform_app_id = Column(
        Text,
        nullable=False,
        comment='플랫폼 앱 ID (store ID / package name)'
    )
    app_name = Column(
        Text,
        comment='앱 이름 (optional, from crawl)'
    )
    storage_path = Column(
        Text,
        nullable=False,
        unique=True,
        comment='Parquet 파일 경로 (UNIQUE)'
    )
    file_format = Column(
        Text,
        nullable=False,
        default='parquet',
        comment='파일 포맷 (기본: parquet)'
    )
    record_count = Column(
        Integer,
        nullable=False,
        default=0,
        comment='배치 내 리뷰 레코드 수'
    )
    content_hash = Column(
        Text,
        comment='파일 내용 해시 (중복 검사용)'
    )
    status = Column(
        SQLEnum(IngestionBatchStatusType, name='ingestion_batch_status_type', create_type=False),
        nullable=False,
        default=IngestionBatchStatusType.PENDING,
        comment='PENDING / LOADED / FAILED / RETRYING / DEAD_LETTER'
    )
    retry_count = Column(
        Integer,
        nullable=False,
        default=0,
        comment='적재 재시도 횟수'
    )
    max_retries = Column(
        Integer,
        nullable=False,
        default=3,
        comment='최대 재시도 횟수'
    )
    error_message = Column(
        Text,
        comment='적재 실패 사유'
    )
    created_at = Column(
        DateTime(timezone=True),
        nullable=False,
        comment='배치 생성 시각 (Parquet 쓰기 완료 시각)'
    )
    updated_at = Column(
        DateTime(timezone=True),
        nullable=False,
        comment='배치 상태 수정 시각'
    )
    loaded_at = Column(
        DateTime(timezone=True),
        comment='DB 적재 완료 시각'
    )

    def __repr__(self):
        return (
            f"<IngestionBatch(batch_id={self.batch_id}, "
            f"storage_path='{self.storage_path}', "
            f"status={self.status}, "
            f"record_count={self.record_count})>"
        )
