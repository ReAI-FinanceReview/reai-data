"""일별 리뷰 목록 서빙 마트 테이블 (Gold - Wide Table)

대시보드의 리뷰 목록 API를 위한 비정규화 와이드 테이블입니다.
reviews_preprocessed + review_action_analysis + review_aspects를 JOIN하여
날짜 기준으로 파티셔닝합니다.

파티션 전략: PARTITION BY RANGE(date) — 파티션은 사전에 생성되어야 합니다.
"""

from sqlalchemy import Column, Date, Text, Boolean, Float, SmallInteger, ARRAY, TIMESTAMP
from sqlalchemy.dialects.postgresql import UUID

from .base import Base


class SrvDailyReviewList(Base):
    """일별 리뷰 목록 서빙 마트 (비정규화 와이드 테이블)

    파티션 키: date
    조회 최적화: (date, service_id), (date, is_action_required)
    """
    __tablename__ = 'srv_daily_review_list'

    # -- 식별자 (복합 PK — 파티션 테이블)
    review_id = Column(
        UUID(as_uuid=True),
        primary_key=True,
        comment='리뷰 고유 ID',
    )
    date = Column(Date, primary_key=True, nullable=False, comment='리뷰 작성일 (파티션 키)')

    # -- 서비스 컨텍스트
    service_id = Column(UUID(as_uuid=True), comment='서비스 ID')

    # -- 리뷰 원본
    refined_text = Column(Text, comment='전처리된 리뷰 텍스트')
    review_summary = Column(Text, comment='LLM 1문장 요약')
    rating = Column(SmallInteger, comment='평점 (1~5)')
    reviewed_at = Column(TIMESTAMP(timezone=True), comment='리뷰 작성 시각 (UTC)')

    # -- 분석 결과
    sentiment_score = Column(Float, comment='평균 감성 점수 (from review_aspects)')
    is_action_required = Column(Boolean, comment='조치 필요 여부')
    is_attention_required = Column(Boolean, comment='관심 필요 여부')

    # -- 배정 결과
    assigned_dept = Column(ARRAY(Text), comment='배정 부서 목록 (from reviews_assigned)')
    keyword = Column(ARRAY(Text), comment='키워드 배열 (from review_aspects)')
    confidence = Column(Float, comment='배정 확률 (from reviews_assigned)')

    def __repr__(self):
        return (
            f"<SrvDailyReviewList(review_id={self.review_id}, date={self.date}, "
            f"service_id={self.service_id}, action={self.is_action_required})>"
        )
