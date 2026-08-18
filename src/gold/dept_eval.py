"""Gold Layer - 부서 배정 평가 (Assignment Evaluation)

배정 결과를 사람이 매긴 정답 라벨과 대조한다.

**평가 축이 라벨인 이유.** 애초 설계는 규칙 배정기(이슈 #40 원 스펙)를
베이스라인으로 두고 LLM 배정의 승률을 재는 것이었다. 그러나 실측에서 규칙
배정기는 ANALYZED 621건 중 533건(85.8%)을 미분류로 남겼다. organizations
키워드 651개 중 리뷰 키워드와 겹치는 것이 23개(3.5%)뿐이기 때문이다 —
사용자는 증상을 쓰고(``로그인이 안 됨``) 조직도는 업무 영역을 쓴다
(``디지털채널 총괄``). 거의 전부 기권하는 베이스라인을 이기는 것은 변별력이
없으므로, 주 지표를 **라벨 대비 절대 정확도**로 두고 규칙 비교는 참고 지표로
남긴다.

**정답과 예측의 모양이 다르다.** 라벨은 org_id 하나이고, 예측은 상위 경로를
포함한 배열이다(``['1', '1-1', '1-1-2']``). "최하위까지 판별하지 못하면 적당한
상위 조직을 고르는 것이 더 적절할 수 있다"는 정책을 채택했으므로, 상위에서
멈춘 판정을 오답으로 처리하면 정책과 지표가 서로 싸운다. 그래서 일치를 단일
불리언이 아니라 계층 관계로 나눠 센다.

| kind | 뜻 |
|---|---|
| ``exact`` | 예측 말단 == 정답 |
| ``ancestor`` | 예측이 정답의 상위에서 멈춤 (정책상 허용되는 보수적 판정) |
| ``descendant`` | 예측이 정답보다 더 내려감 (과잉 확신) |
| ``top_level`` | 최상위 본부만 같음 |
| ``miss`` | 다른 줄기 |
| ``unclassified`` | 미분류 (기권) |
| ``missing`` | 배정 결과 자체가 없음 |
"""

from __future__ import annotations

import csv
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Mapping, Sequence

from src.gold.dept_assigner import UNCLASSIFIED, Assignment, expand_org_path

# 라벨 CSV 기본 위치. --labels 인자나 DEPT_LABELS_PATH 환경변수로 덮어쓴다.
DEFAULT_LABELS_PATH = Path("tests/fixtures/dept_labels.csv")

# 라벨 CSV 열 이름. 라벨링 시트와 같은 헤더를 쓴다.
COL_REVIEW_ID = "review_id"
COL_LABEL = "assigned_dept"
COL_STOPPED_AT_PARENT = "stopped_at_parent"
COL_AMBIGUOUS = "ambiguous"
COL_MEMO = "memo"

# 계층상 같은 줄기로 셈하는 판정. 정책상 상위에서 멈추는 것을 허용하므로
# ancestor 를 오답으로 두지 않는다.
HIERARCHY_KINDS = ("exact", "ancestor", "descendant")

_TRUTHY = {"1", "y", "yes", "true", "t", "o", "ㅇ"}


@dataclass
class Label:
    """사람이 매긴 정답 하나."""

    review_id: str
    org_id: str
    stopped_at_parent: bool = False
    ambiguous: bool = False
    memo: str = ""


def _is_truthy(value: str) -> bool:
    return value.strip().lower() in _TRUTHY


def load_labels(path: Path | str) -> List[Label]:
    """라벨 CSV 를 읽는다. ``assigned_dept`` 가 빈 행은 미라벨로 보고 건너뛴다.

    라벨링 시트가 Excel 을 거치면 BOM 이 붙으므로 utf-8-sig 로 연다.
    """
    labels: List[Label] = []
    with Path(path).open(encoding="utf-8-sig", newline="") as fp:
        reader = csv.DictReader(fp)
        if reader.fieldnames is None or COL_LABEL not in reader.fieldnames:
            raise ValueError(f"라벨 CSV 에 '{COL_LABEL}' 열이 없다: {path}")

        for row in reader:
            org_id = (row.get(COL_LABEL) or "").strip()
            review_id = (row.get(COL_REVIEW_ID) or "").strip()
            if not org_id or not review_id:
                continue
            labels.append(
                Label(
                    review_id=review_id,
                    org_id=org_id,
                    stopped_at_parent=_is_truthy(row.get(COL_STOPPED_AT_PARENT) or ""),
                    ambiguous=_is_truthy(row.get(COL_AMBIGUOUS) or ""),
                    memo=(row.get(COL_MEMO) or "").strip(),
                )
            )
    return labels


def match_kind(predicted: Sequence[str] | None, label: str) -> str:
    """예측 경로와 정답 org_id 의 계층 관계를 판정한다."""
    if not predicted:
        return "missing"
    if list(predicted) == [UNCLASSIFIED]:
        return "unclassified"

    leaf = predicted[-1]
    if leaf == label:
        return "exact"
    if leaf in expand_org_path(label):
        return "ancestor"
    if label in expand_org_path(leaf):
        return "descendant"
    if leaf.split("-")[0] == label.split("-")[0]:
        return "top_level"
    return "miss"


@dataclass
class EvalResult:
    """평가 집계. 비율은 라벨 전체를 분모로 한다."""

    counts: Dict[str, int] = field(default_factory=dict)
    per_review: Dict[str, str] = field(default_factory=dict)
    ambiguous_ids: List[str] = field(default_factory=list)

    @property
    def total(self) -> int:
        return sum(self.counts.values())

    def _rate(self, kinds: Sequence[str]) -> float:
        if self.total == 0:
            return 0.0
        return round(sum(self.counts.get(k, 0) for k in kinds) / self.total, 4)

    @property
    def exact_rate(self) -> float:
        return self._rate(("exact",))

    @property
    def hierarchy_rate(self) -> float:
        """같은 줄기(exact/ancestor/descendant) 비율. 정책상의 실질 정확도다."""
        return self._rate(HIERARCHY_KINDS)

    @property
    def top_level_rate(self) -> float:
        """최상위 본부까지 맞춘 비율. 계층 정확도의 상한선."""
        return self._rate(HIERARCHY_KINDS + ("top_level",))

    @property
    def unclassified_rate(self) -> float:
        return self._rate(("unclassified",))

    def as_dict(self) -> dict:
        return {
            "total": self.total,
            "counts": dict(self.counts),
            "exact_rate": self.exact_rate,
            "hierarchy_rate": self.hierarchy_rate,
            "top_level_rate": self.top_level_rate,
            "unclassified_rate": self.unclassified_rate,
            "ambiguous": len(self.ambiguous_ids),
        }


def evaluate(labels: Sequence[Label], assignments: Mapping[str, Assignment]) -> EvalResult:
    """라벨과 저장된 배정 결과를 대조한다.

    ``ambiguous`` 표시된 라벨도 분모에 포함한다. 빼면 어려운 건을 지운 만큼
    점수가 올라가므로, 세어서 함께 보고하되 분모는 건드리지 않는다.
    """
    counts: Dict[str, int] = {}
    per_review: Dict[str, str] = {}
    ambiguous_ids: List[str] = []

    for label in labels:
        assignment = assignments.get(label.review_id)
        kind = match_kind(assignment.assigned_dept if assignment else None, label.org_id)
        counts[kind] = counts.get(kind, 0) + 1
        per_review[label.review_id] = kind
        if label.ambiguous:
            ambiguous_ids.append(label.review_id)

    return EvalResult(counts=counts, per_review=per_review, ambiguous_ids=ambiguous_ids)


def format_report(result: EvalResult, *, title: str = "부서 배정 평가") -> str:
    """사람이 읽는 표. 비율만 내면 검산할 수 없으므로 건수를 함께 낸다."""
    order = ["exact", "ancestor", "descendant", "top_level", "miss", "unclassified", "missing"]
    lines = [
        f"{title}",
        f"라벨 {result.total}건 (ambiguous {len(result.ambiguous_ids)}건 포함)",
        "",
        f"{'kind':<14}{'count':>7}{'rate':>9}",
        "-" * 30,
    ]
    for kind in order:
        count = result.counts.get(kind, 0)
        rate = count / result.total if result.total else 0.0
        lines.append(f"{kind:<14}{count:>7}{rate:>9.1%}")

    lines += [
        "-" * 30,
        f"{'exact':<14}{'':>7}{result.exact_rate:>9.1%}   말단 정확도",
        f"{'hierarchy':<14}{'':>7}{result.hierarchy_rate:>9.1%}   같은 줄기 (주 지표)",
        f"{'top_level':<14}{'':>7}{result.top_level_rate:>9.1%}   본부까지 일치",
        f"{'unclassified':<14}{'':>7}{result.unclassified_rate:>9.1%}   기권",
    ]
    return "\n".join(lines)
