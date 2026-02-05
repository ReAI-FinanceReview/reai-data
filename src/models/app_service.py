"""논리적 서비스 마스터 모델

This module defines the AppService model representing logical service masters
that group multiple physical app instances across platforms.
"""

from sqlalchemy import Column, Text
from sqlalchemy.dialects.postgresql import UUID

from .base import Base


class AppService(Base):
    """논리적 서비스 마스터 테이블

    앱 서비스의 논리적 단위를 정의합니다.
    예: '카카오뱅크' 서비스는 App Store와 Play Store에 각각 물리적 앱을 가질 수 있습니다.
    """
    __tablename__ = 'app_service'

    service_id = Column(UUID(as_uuid=True), primary_key=True, comment='서비스 고유 ID (UUID v7)')
    service_name = Column(Text, comment='서비스 이름 (예: 카카오뱅크)')

    def __repr__(self):
        return f"<AppService(service_id={self.service_id}, service_name='{self.service_name}')>"
