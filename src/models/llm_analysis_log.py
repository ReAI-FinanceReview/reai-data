"""
LLM 분석 로그 모델 (Silver Layer)
"""
from sqlalchemy import Column, String, Integer, DateTime, Text, BigInteger, Float, JSON
from .base import Base


class LLMAnalysisLog(Base):
    """LLM 분석 과정 로깅 모델"""
    __tablename__ = 'review_llm_analysis_logs'

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    review_id = Column(String, nullable=False)
    app_id = Column(String, nullable=False)
    platform = Column(String, nullable=False)

    # 분석 타입
    analysis_type = Column(String, nullable=False)  # 'sentiment', 'keyword', 'topic', 'embedding'

    # 모델 정보
    model_provider = Column(String, nullable=False)  # 'openai', 'huggingface', 'anthropic'
    model_name = Column(String, nullable=False)  # 'gpt-4', 'claude-3', 'bert-base'
    model_version = Column(String)

    # 요청/응답 정보
    input_text = Column(Text)
    input_tokens = Column(Integer)
    output_result = Column(JSON)
    output_tokens = Column(Integer)

    # 성능 메트릭
    latency_ms = Column(Integer)  # 응답 시간 (밀리초)
    cost_usd = Column(Float)  # API 호출 비용 (USD)

    # 상태 및 에러
    status = Column(String, nullable=False, default='success')  # 'success', 'failed', 'partial'
    error_message = Column(Text)

    # 품질 메트릭
    confidence_score = Column(Float)
    quality_score = Column(Float)

    # 메타데이터
    created_at = Column(DateTime, nullable=False)
    processing_pipeline_version = Column(String, default='1.0')
    additional_metadata = Column(JSON)
