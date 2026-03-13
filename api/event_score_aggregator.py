from __future__ import annotations

"""
事件评分聚合器

用途：
- 接收真实事件列表
- 按标的聚合成 Agent 可消费的 score_inputs
"""

from typing import Any, Dict, List


class EventScoreAggregator:
    def aggregate_for_symbols(self, events: List[Dict[str, Any]]) -> Dict[str, Dict[str, float]]:
        """
        输入事件列表示例：
        {
            "event_class": "PolicyEvent" | "AnnouncementEvent" | "MarketNewsEvent",
            "symbols": ["000001.SZ"],
            "strength": 0.8,
            "direction": "positive" | "negative" | "neutral",
            "source_name": "...",
            "title": "...",
            "source_level": "L1" | "L2" | ...
        }
        """
        scores: Dict[str, Dict[str, float]] = {}

        for event in events:
            symbols = event.get("symbols", []) or []
            if not symbols:
                continue

            event_class = event.get("event_class", "")
            strength = float(event.get("strength", 0.0) or 0.0)
            direction = (event.get("direction", "neutral") or "neutral").lower()
            signed_strength = self._signed_strength(strength, direction)

            for code in symbols:
                bucket = scores.setdefault(code, {
                    "policy_score": 0.0,
                    "event_score": 0.0,
                    "technical_score": 0.0,
                    "intl_adjustment": 0.0,
                })

                if event_class == "PolicyEvent":
                    bucket["policy_score"] += signed_strength * 100.0
                elif event_class == "AnnouncementEvent":
                    bucket["event_score"] += signed_strength * 100.0
                elif event_class == "MarketNewsEvent":
                    # 先简单记到 event_score；若标记国际事件则进 intl_adjustment
                    if self._is_international_mapping_event(event):
                        bucket["intl_adjustment"] += signed_strength * 100.0
                    else:
                        bucket["event_score"] += signed_strength * 100.0

        # 限幅到 0-100 / -100-100 区间后再裁成更适合当前主链路的区间
        normalized: Dict[str, Dict[str, float]] = {}
        for code, item in scores.items():
            normalized[code] = {
                "policy_score": self._cap_0_100(item["policy_score"]),
                "event_score": self._cap_0_100(item["event_score"]),
                "technical_score": 0.0,
                "intl_adjustment": self._cap_neg100_100(item["intl_adjustment"]),
            }
        return normalized

    @staticmethod
    def _signed_strength(strength: float, direction: str) -> float:
        if direction in {"positive", "bullish", "up"}:
            return max(0.0, strength)
        if direction in {"negative", "bearish", "down"}:
            return -max(0.0, strength)
        return 0.0

    @staticmethod
    def _is_international_mapping_event(event: Dict[str, Any]) -> bool:
        title = (event.get("title", "") or "").lower()
        content = (event.get("content", "") or "").lower()
        keywords = ["fed", "tariff", "export control", "geopolit", "commodity", "brent", "wti"]
        return any(k in title or k in content for k in keywords)

    @staticmethod
    def _cap_0_100(value: float) -> float:
        return max(0.0, min(100.0, value))

    @staticmethod
    def _cap_neg100_100(value: float) -> float:
        return max(-100.0, min(100.0, value))
