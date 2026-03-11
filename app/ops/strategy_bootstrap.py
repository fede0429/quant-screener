from __future__ import annotations

from app.core.strategy_registry import StrategyRegistry


def build_default_registry() -> StrategyRegistry:
    registry = StrategyRegistry()
    registry.register(
        "quality_growth_v2",
        {
            "universe": "A_SHARE_MAIN",
            "rebalance_frequency": "weekly",
            "top_n": 20,
            "paper_only": True,
        },
    )
    return registry
