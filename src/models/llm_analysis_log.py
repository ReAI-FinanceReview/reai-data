"""LLM 분석 로그 모델 (Audit)

This module defines the LLMAnalysisLog model for tracking LLM API calls and results.
"""

from sqlalchemy import Column, String, Integer, DateTime, Text, Enum as SQLEnum
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.sql import func

from .base import Base
from .enums import AnalysisStatusType


class LLMAnalysisLog(Base):
    """LLM 분석 로그 테이블 (Audit)

    LLM API 호출 및 분석 결과를 추적하는 감사 로그 테이블입니다.
    """
    __tablename__ = 'review_llm_analysis_logs'

    id = Column(
        Integer,
        primary_key=True,
        autoincrement=True,
        comment='로그 ID'
    )
    source_table = Column(Text, comment='소스 테이블 이름')
    source_record_id = Column(
        Text,
        comment='소스 레코드 ID (UUID 또는 INT 지원)'
    )
    model_name = Column(Text, comment='사용한 모델 이름')
    params = Column(Text, comment='모델 파라미터 (JSON 문자열)')
    result_payload = Column(JSONB, comment='분석 결과 (JSONB)')
    status = Column(
        SQLEnum(AnalysisStatusType, name='analysis_status_type', create_type=False),
        comment='분석 상태 (PENDING, PROCESSING, SUCCESS, FAILED)'
    )
    error_message = Column(Text, comment='에러 메시지 (실패 시)')
    processed_at = Column(
        DateTime(timezone=True),
        comment='처리 완료 시각'
    )
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
        return (
            f"<LLMAnalysisLog(id={self.id}, source_table='{self.source_table}', "
            f"source_id='{self.source_record_id}', status={self.status})>"
        )
