from __future__ import annotations

from typing import Dict, List
from api.models.shadow_models import ShadowPosition


class ShadowReplayEngine:
    def replay(self, position: ShadowPosition, price_path: List[float]) -> Dict:
        if not price_path:
            return {
                "shadow_id": position.shadow_id,
                "replayed": False,
                "reason": "missing_price_path",
            }

        entry = position.entry_price if position.entry_price is not None else price_path[0]
        if entry is None or entry <= 0:
            return {
                "shadow_id": position.shadow_id,
                "replayed": False,
                "reason": "invalid_entry_price",
            }

        def ret_at(idx: int):
            if idx >= len(price_path):
                return None
            return round((price_path[idx] - entry) / entry * 100.0, 4)

        result = {
            "shadow_id": position.shadow_id,
            "replayed": True,
            "entry_price": entry,
            "return_t1": ret_at(1),
            "return_t3": ret_at(3),
            "return_t5": ret_at(5),
            "max_favorable_excursion": round((max(price_path) - entry) / entry * 100.0, 4),
            "max_adverse_excursion": round((min(price_path) - entry) / entry * 100.0, 4),
            "hit_stop_flag": False,
            "hit_tp_flag": False,
        }

        if position.metadata.get("planned_stop") is not None:
            stop = float(position.metadata["planned_stop"])
            result["hit_stop_flag"] = any(p <= stop for p in price_path if p is not None)
        if position.metadata.get("planned_tp") is not None:
            tp = float(position.metadata["planned_tp"])
            result["hit_tp_flag"] = any(p >= tp for p in price_path if p is not None)

        return result
