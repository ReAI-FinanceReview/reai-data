"""
앱 데이터 모델
"""
from sqlalchemy import Column, BigInteger, String, Boolean, Date, DateTime
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.sql import func
from .base import Base

class App(Base):
    """앱 데이터 모델 (apps 테이블)"""
    __tablename__ = 'apps'

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    logical_app_id = Column(UUID(as_uuid=True), nullable=True)
    group_id = Column(UUID(as_uuid=True), nullable=False)
    name = Column(String, nullable=False)
    appstore_id = Column(String, nullable=True)
    playstore_id = Column(String, nullable=True)
    app_type = Column(String, nullable=True)
    valid_from = Column(Date, nullable=False)
    valid_to = Column(Date, nullable=True)
    is_active = Column(Boolean, nullable=True, default=True)
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now())
