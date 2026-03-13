from __future__ import annotations

"""
真实事件评分桥接层

用途：
- 把真实事件列表聚合成 score_inputs
- 调用 BaseScreenerRuntimeBridge 完成 Agent 主链路真实接入
"""

from typing import Any, Dict, List, Optional

from api.base_screener_runtime_bridge import BaseScreenerRuntimeBridge
from api.event_score_aggregator import EventScoreAggregator


class EventScoreBridge:
    def __init__(
        self,
        runtime_bridge: BaseScreenerRuntimeBridge,
        aggregator: Optional[EventScoreAggregator] = None,
    ) -> None:
        self.runtime_bridge = runtime_bridge
        self.aggregator = aggregator or EventScoreAggregator()

    def run_with_events(
        self,
        trade_date: str,
        next_trade_date: str,
        base_rows: List[Dict[str, Any]],
        event_rows: List[Dict[str, Any]],
        quantity: int = 100,
    ) -> Dict[str, Any]:
        score_inputs = self.aggregator.aggregate_for_symbols(event_rows)
        return self.runtime_bridge.run_from_rows(
            trade_date=trade_date,
            next_trade_date=next_trade_date,
            base_rows=base_rows,
            quantity=quantity,
            score_inputs=score_inputs,
        )
