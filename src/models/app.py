"""물리적 앱 인스턴스 모델

This module defines the App model representing physical app instances
on specific platforms (App Store or Play Store).
"""

import enum

from sqlalchemy import Column, Text, Enum as SQLEnum
from sqlalchemy.dialects.postgresql import UUID

from .base import Base


class PlatformType(enum.Enum):
    """플랫폼 타입 ENUM"""
    APPSTORE = "APPSTORE"
    PLAYSTORE = "PLAYSTORE"


class App(Base):
    """물리적 앱 인스턴스 테이블

    각 플랫폼(App Store, Play Store)의 물리적 앱 인스턴스를 표현합니다.
    논리적 서비스 연결은 app_metadata 테이블을 통해 관리됩니다.
    """
    __tablename__ = 'apps'

    app_id = Column(
        UUID(as_uuid=True),
        primary_key=True,
        comment='앱 고유 ID (UUID v7)'
    )
    platform_app_id = Column(
        Text,
        nullable=False,
        comment='플랫폼별 앱 ID (App Store ID 또는 Play Store 패키지명)'
    )
    platform = Column(
        SQLEnum(PlatformType, name='platform_type', create_type=False),
        comment='플랫폼 타입 (APPSTORE, PLAYSTORE)'
    )
    name = Column(
        Text,
        nullable=False,
        comment='앱 이름'
    )

    def __repr__(self):
        return (
            f"<App(app_id={self.app_id}, name='{self.name}', "
            f"platform={self.platform}, platform_app_id='{self.platform_app_id}')>"
        )
