"""
업무 할당 데이터 모델 (Gold Layer)
ERD 기반으로 정의됨
"""
from sqlalchemy import Column, String, Integer, DateTime, Text, BigInteger, Float, ForeignKey
from .base import Base


class ReviewAssigned(Base):
    """
    업무 할당 데이터 모델 (Gold Layer)

    LLM 팀원이 구현할 업무 할당 로직의 결과를 저장하는 테이블
    reviews_features의 키워드, 토픽, 감성 등을 바탕으로
    리뷰를 처리할 담당 부서를 결정
    """
    __tablename__ = 'reviews_assigned'

    # Primary Key
    id = Column(BigInteger, primary_key=True, autoincrement=True)

    # Foreign Key to reviews_features
    feat_id = Column(BigInteger, ForeignKey('reviews_features.id'), nullable=False)

    # 업무 할당 정보
    assigned_dept = Column(String, nullable=False, comment='AI Agent 배정 부서')
    assignment_reason = Column(Text, comment='배정 사유')
    
    # 신뢰도 및 상태
    confidence = Column(Float, comment='배정 확률/신뢰도 (0.0 ~ 1.0)')
    failed_yn = Column(String, default='N', comment='성공여부 (Y/N)')
    try_number = Column(Integer, default=1, comment='몇 번째 시도만에 성공했는지')

    # 타임스탬프
    created_at = Column(DateTime, nullable=False)
    updated_at = Column(DateTime)


class DepartmentMapping(Base):
    """
    부서 매핑 참조 테이블

    금융 앱 리뷰를 처리할 수 있는 부서/팀 정보
    """
    __tablename__ = 'department_mappings'

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    dept_code = Column(String, unique=True, nullable=False, comment='부서 코드')
    dept_name = Column(String, nullable=False, comment='부서명')
    dept_category = Column(String, comment='부서 카테고리 (tech/cs/security/etc)')
    
    # 담당 토픽/키워드
    responsible_topics = Column(Text, comment='담당 토픽 (JSON 배열)')
    responsible_keywords = Column(Text, comment='담당 키워드 (JSON 배열)')
    
    # 우선순위
    priority = Column(Integer, default=50, comment='우선순위 (높을수록 우선)')
    
    # 상태
    active = Column(String, default='Y', comment='활성 여부 (Y/N)')
    
    # 타임스탬프
    created_at = Column(DateTime, nullable=False)
    updated_at = Column(DateTime)
