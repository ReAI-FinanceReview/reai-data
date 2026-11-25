"""Gold Layer - 분석 제외(SKIPPED) 신호

규칙 기반 ABSA 가 분석할 수 없는 리뷰는 실패가 아니라 종결 상태 ``SKIPPED`` 로 남긴다
(issue #94). 재시도해도 결과가 같으므로 ``retry_count`` 를 올리지 않고, 사유는
``review_master_index.error_message`` 에 ``skip:<사유>`` 로 기록한다.

이 모듈은 의존성이 없다. ``GoldOrchestrator`` 는 분석기 모듈을 지연 임포트하므로
(무거운 선택 의존성 격리) 오케스트레이터가 잡아야 하는 예외를 분석기 모듈에 두지 않는다.
"""

from __future__ import annotations

SKIP_EMPTY = "empty"
SKIP_NON_KOREAN = "non_korean"
SKIP_NO_ASPECT = "no_aspect"


class AnalysisSkipped(Exception):
    """리뷰를 분석 대상에서 제외한다. 종결 상태이며 재시도하지 않는다."""

    def __init__(self, reason: str):
        super().__init__(reason)
        self.reason = reason

    @property
    def error_message(self) -> str:
        return f"skip:{self.reason}"
