from __future__ import annotations

"""
技术评分聚合器

用途：
- 接收真实技术面 / 尾盘确认特征
- 聚合成 Agent 可消费的 technical_score
"""

from typing import Any, Dict, List


class TechnicalScoreAggregator:
    def aggregate_for_symbols(self, feature_rows: List[Dict[str, Any]]) -> Dict[str, Dict[str, float]]:
        """
        输入行示例：
        {
            "code": "000001.SZ",
            "rps": 92,
            "close_vs_high": 0.97,
            "last_30m_amount_ratio": 0.28,
            "breakout_confirmed": True,
            "sector_rank_pct": 0.92,
            "volume_expansion": 1.8
        }
        """
        result: Dict[str, Dict[str, float]] = {}

        for row in feature_rows:
            code = row.get("code")
            if not code:
                continue

            score = 0.0

            rps = float(row.get("rps", 0.0) or 0.0)
            close_vs_high = float(row.get("close_vs_high", 0.0) or 0.0)
            last_30m_amount_ratio = float(row.get("last_30m_amount_ratio", 0.0) or 0.0)
            sector_rank_pct = float(row.get("sector_rank_pct", 0.0) or 0.0)
            volume_expansion = float(row.get("volume_expansion", 0.0) or 0.0)
            breakout_confirmed = bool(row.get("breakout_confirmed", False))

            score += min(30.0, rps * 0.30)
            score += min(20.0, max(0.0, close_vs_high) * 20.0)
            score += min(15.0, max(0.0, last_30m_amount_ratio) * 50.0)
            score += min(15.0, max(0.0, sector_rank_pct) * 15.0)
            score += min(10.0, max(0.0, volume_expansion) * 4.0)
            if breakout_confirmed:
                score += 10.0

            result[code] = {
                "policy_score": 0.0,
                "event_score": 0.0,
                "technical_score": max(0.0, min(100.0, score)),
                "intl_adjustment": 0.0,
            }
        return result
