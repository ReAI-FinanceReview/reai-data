"""사전 데이터 모델 (Reference Data)

This module defines dictionary models for synonyms, profanity, and financial terms.
"""

from sqlalchemy import Column, String, Integer, DateTime, Text, BigInteger, Boolean, SmallInteger
from sqlalchemy.sql import func

from .base import Base


class Synonym(Base):
    """동의어 사전 모델"""
    __tablename__ = 'synonyms'

    id = Column(Integer, primary_key=True, autoincrement=True, comment='동의어 ID')
    variant_form = Column(Text, nullable=False, comment='변형된 형태 (예: "신한 슈퍼 SOL")')
    canonical_form = Column(Text, nullable=False, comment='표준 형태 (예: "신한슈퍼솔")')
    normalized_form = Column(Text, comment='정규화 형태 (optional)')
    is_active = Column(Boolean, nullable=False, server_default='true', comment='활성 여부')
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
        return f"<Synonym(id={self.id}, variant='{self.variant_form}', canonical='{self.canonical_form}')>"


class Profanity(Base):
    """욕설/비속어 사전 모델"""
    __tablename__ = 'profanities'

    id = Column(Integer, primary_key=True, autoincrement=True, comment='비속어 ID')
    word = Column(Text, nullable=False, comment='비속어 단어')
    normalized_form = Column(Text, comment='정규화 형태 (optional)')
    severity_level = Column(SmallInteger, nullable=False, comment='심각도 (1=낮음, 2=중간, 3=높음)')
    is_active = Column(Boolean, nullable=False, server_default='true', comment='활성 여부')
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
        return f"<Profanity(id={self.id}, word='{self.word}', severity={self.severity_level})>"


class FinancialTerm(Base):
    """금융 용어 사전 모델 (custom table, not in schema_v2.sql)"""
    __tablename__ = 'financial_terms'

    id = Column(BigInteger, primary_key=True, autoincrement=True, comment='용어 ID')
    term = Column(String, nullable=False, unique=True, index=True, comment='금융 용어')
    definition = Column(Text, comment='용어 정의')
    category = Column(String, comment='카테고리 (banking, investment, insurance, payment)')
    importance = Column(Integer, default=50, comment='중요도 (0-100)')
    created_at = Column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        comment='생성 시각'
    )
    updated_at = Column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        comment='수정 시각'
    )

    def __repr__(self):
        return f"<FinancialTerm(id={self.id}, term='{self.term}', category='{self.category}')>"
