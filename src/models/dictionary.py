"""
사전 데이터 모델 (Reference Data)
"""
from sqlalchemy import Column, String, Integer, DateTime, Text, BigInteger, Boolean
from .base import Base


class Synonym(Base):
    """동의어 사전 모델 (matches DBinit.sql schema)"""
    __tablename__ = 'synonyms'

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    variant_form = Column(String, nullable=False)  # 변형된 형태 (예: "신한 슈퍼 SOL")
    canonical_form = Column(String, nullable=False)  # 표준 형태 (예: "신한슈퍼솔")
    normalized_form = Column(String)  # 정규화 형태 (optional)
    is_active = Column(Boolean, nullable=False, default=True)
    created_at = Column(DateTime, nullable=False)
    updated_at = Column(DateTime, nullable=False)


class Profanity(Base):
    """욕설/비속어 사전 모델 (matches DBinit.sql schema)"""
    __tablename__ = 'profanities'

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    word = Column(String, nullable=False)
    normalized_form = Column(String)  # 정규화 형태 (optional)
    severity_level = Column(Integer, nullable=False)  # 1=low, 2=medium, 3=high
    is_active = Column(Boolean, nullable=False, default=True)
    created_at = Column(DateTime, nullable=False)
    updated_at = Column(DateTime, nullable=False)


class FinancialTerm(Base):
    """금융 용어 사전 모델"""
    __tablename__ = 'financial_terms'

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    term = Column(String, nullable=False, unique=True, index=True)
    definition = Column(Text)
    category = Column(String)  # 'banking', 'investment', 'insurance', 'payment'
    importance = Column(Integer, default=50)  # 0-100
    created_at = Column(DateTime, nullable=False)
    updated_at = Column(DateTime)
