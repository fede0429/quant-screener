from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional

from api.models.base_candidate import BaseCandidate
from api.models.decision_input import DecisionInput


@dataclass
class BaseScreenerAdapterConfig:
    core_threshold: float = 80.0
    watch_threshold: float = 60.0
    block_if_has_st: bool = True
    block_if_has_regulatory: bool = True
    block_if_has_major_reduction: bool = True
    block_if_has_major_unlock: bool = True


class BaseScreenerAdapter:
    def __init__(self, config: Optional[BaseScreenerAdapterConfig] = None) -> None:
        self.config = config or BaseScreenerAdapterConfig()

    def normalize_candidates(self, rows: List[Dict]) -> List[BaseCandidate]:
        candidates: List[BaseCandidate] = []
        for row in rows:
            candidate = BaseCandidate(
                code=row["code"],
                exchange=row.get("exchange") or self._infer_exchange(row["code"]),
                name=row.get("name", ""),
                sector=row.get("sector", ""),
                industry=row.get("industry", ""),
                base_selection_score=self._derive_base_selection_score(row),
                model_score=float(row.get("model_score", 0.0) or 0.0),
                composite_score=float(row.get("composite_score", 0.0) or 0.0),
                risk_flags=list(row.get("risk_flags", [])),
                tags=list(row.get("tags", [])),
                extra=dict(row),
            )
            candidate.candidate_bucket = self._bucketize(candidate)
            candidates.append(candidate)
        candidates.sort(key=lambda x: x.base_selection_score, reverse=True)
        return candidates

    def split_buckets(self, candidates: List[BaseCandidate]) -> Dict[str, List[BaseCandidate]]:
        return {"core": [x for x in candidates if x.candidate_bucket == "core"],
                "watch": [x for x in candidates if x.candidate_bucket == "watch"],
                "blocked": [x for x in candidates if x.candidate_bucket == "blocked"]}

    def to_decision_input(self, trade_date: str, candidate: BaseCandidate, policy_score: float = 0.0,
                          event_score: float = 0.0, technical_score: float = 0.0, intl_adjustment: float = 0.0,
                          thesis: str = "", reference_price: float | None = None, latest_price: float | None = None,
                          extra: Optional[Dict] = None) -> DecisionInput:
        return DecisionInput(
            trade_date=trade_date, code=candidate.code, exchange=candidate.exchange,
            base_selection_score=candidate.base_selection_score, policy_score=policy_score, event_score=event_score,
            technical_score=technical_score, intl_adjustment=intl_adjustment, thesis=thesis, tags=list(candidate.tags),
            reference_price=reference_price, latest_price=latest_price, risk_flags=list(candidate.risk_flags),
            extra={"base_candidate": candidate.to_dict(), **(extra or {})},
        )

    def _derive_base_selection_score(self, row: Dict) -> float:
        if row.get("base_selection_score") is not None:
            return float(row["base_selection_score"])
        if row.get("final_score") is not None:
            return float(row["final_score"])
        model_score = float(row.get("model_score", 0.0) or 0.0)
        composite_score = float(row.get("composite_score", 0.0) or 0.0)
        if model_score > 0:
            return round(composite_score * 0.65 + model_score * 0.35, 4)
        return composite_score

    def _bucketize(self, candidate: BaseCandidate) -> str:
        flags = {x.lower() for x in candidate.risk_flags}
        if self.config.block_if_has_st and "st" in flags:
            return "blocked"
        if self.config.block_if_has_regulatory and "regulatory" in flags:
            return "blocked"
        if self.config.block_if_has_major_reduction and "major_reduction" in flags:
            return "blocked"
        if self.config.block_if_has_major_unlock and "major_unlock" in flags:
            return "blocked"
        if candidate.base_selection_score >= self.config.core_threshold:
            return "core"
        if candidate.base_selection_score >= self.config.watch_threshold:
            return "watch"
        return "blocked"

    @staticmethod
    def _infer_exchange(code: str) -> str:
        code = (code or "").upper()
        if code.endswith(".SZ"):
            return "SZ"
        if code.endswith(".SH"):
            return "SH"
        return "SZ"
