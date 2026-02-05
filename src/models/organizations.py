"""조직 계층 구조 모델

This module defines the Organization model using PostgreSQL ltree extension
for hierarchical organization structure.
"""

from sqlalchemy import Column, VARCHAR, Text, DateTime, ARRAY
from sqlalchemy.sql import func

from .base import Base


class Organization(Base):
    """조직 계층 구조 테이블 (ltree 사용)

    PostgreSQL ltree 확장을 사용하여 조직 계층 구조를 표현합니다.
    예: '1.1.3' = 본부(1) > 부(1) > 팀(3)
    Note: ltree is stored as Text in SQLAlchemy, actual ltree type is created in DB schema
    """
    __tablename__ = 'organizations'

    org_id = Column(
        Text,
        primary_key=True,
        comment='조직 경로 (ltree 형식, 예: 1.1.3)'
    )
    org_name = Column(VARCHAR, comment='조직 이름')
    role_responsibility = Column(Text, comment='역할 및 책임')
    keywords = Column(ARRAY(Text), comment='키워드 목록')
    review_types = Column(Text, comment='처리 가능한 리뷰 타입')
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
        return f"<Organization(org_id='{self.org_id}', org_name='{self.org_name}')>"
