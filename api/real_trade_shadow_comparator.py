from __future__ import annotations

from typing import Dict


class RealTradeVsShadowComparator:
    def compare(self, real_trade: Dict, shadow_result: Dict) -> Dict:
        if not shadow_result.get("replayed"):
            return {
                "compared": False,
                "reason": shadow_result.get("reason", "shadow_not_replayed"),
            }

        real_return = real_trade.get("realized_return")
        shadow_t3 = shadow_result.get("return_t3")

        diff = None
        if real_return is not None and shadow_t3 is not None:
            diff = round(real_return - shadow_t3, 4)

        return {
            "compared": True,
            "symbol": real_trade.get("symbol"),
            "proposal_id": real_trade.get("proposal_id"),
            "realized_return": real_return,
            "shadow_return_t3": shadow_t3,
            "return_diff": diff,
            "real_trade_status": real_trade.get("status", ""),
            "shadow_hit_stop_flag": shadow_result.get("hit_stop_flag", False),
            "shadow_hit_tp_flag": shadow_result.get("hit_tp_flag", False),
        }
