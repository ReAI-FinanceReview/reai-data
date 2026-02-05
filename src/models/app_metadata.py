"""앱-서비스 연결 및 이력 관리 모델 (SCD Type 2)

This module defines the AppMetadata model that links physical apps to logical services
and tracks historical changes using Slowly Changing Dimension Type 2 pattern.
"""

from sqlalchemy import Column, Integer, Text, Date, Boolean, Enum as SQLEnum, ForeignKey
from sqlalchemy.dialects.postgresql import UUID

from .base import Base
from .enums import AppType


class AppMetadata(Base):
    """앱-서비스 연결 및 이력 관리 테이블 (SCD Type 2)

    물리적 앱과 논리적 서비스를 연결하고, 금융그룹 정보와 이력을 관리합니다.
    SCD Type 2 패턴을 사용하여 변경 이력을 추적합니다.
    """
    __tablename__ = 'app_metadata'

    id = Column(Integer, primary_key=True, autoincrement=True, comment='메타데이터 레코드 ID')
    app_id = Column(
        UUID(as_uuid=True),
        ForeignKey('apps.app_id'),
        nullable=False,
        comment='물리적 앱 ID (FK to apps)'
    )
    service_id = Column(
        UUID(as_uuid=True),
        ForeignKey('app_service.service_id'),
        nullable=False,
        comment='논리적 서비스 ID (FK to app_service)'
    )
    group_id = Column(Text, comment='금융그룹 ID')
    group_type = Column(Text, comment='금융그룹 타입 (은행, 증권, 카드 등)')
    app_type = Column(
        SQLEnum(AppType, name='app_type', create_type=False),
        comment='앱 타입 (CONSUMER, CORPORATE, GLOBAL)'
    )
    valid_from = Column(Date, comment='유효 시작일')
    valid_to = Column(Date, comment='유효 종료일 (NULL이면 현재 유효)')
    is_active = Column(Boolean, comment='활성 여부')

    def __repr__(self):
        return (
            f"<AppMetadata(id={self.id}, app_id={self.app_id}, service_id={self.service_id}, "
            f"is_active={self.is_active})>"
        )
