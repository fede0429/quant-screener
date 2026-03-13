from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional
from uuid import uuid4

from api.models.exit_plan import ExitAction, ExitPlan
from api.utils.market_states import LIMIT_DOWN, LIMIT_UP, OPEN_FLAT, OPEN_HIGH, OPEN_LOW, classify_open_state


@dataclass
class NextDayExitConfig:
    high_open_threshold: float = 0.02
    low_open_threshold: float = -0.02
    first_take_profit_ratio: float = 0.50
    second_take_profit_ratio: float = 0.50
    low_open_stop_ratio: float = 0.50


class NextDayExitEngine:
    def __init__(self, config: Optional[NextDayExitConfig] = None) -> None:
        self.config = config or NextDayExitConfig()

    def build_exit_plan(self, intent_id: str, code: str, trade_date: str, next_trade_date: str) -> ExitPlan:
        return ExitPlan(
            plan_id=str(uuid4()),
            intent_id=intent_id,
            code=code,
            trade_date=trade_date,
            next_trade_date=next_trade_date,
            high_open_plan=self._build_high_open_plan(),
            flat_open_plan=self._build_flat_open_plan(),
            low_open_plan=self._build_low_open_plan(),
            limit_up_plan=self._build_limit_up_plan(),
            limit_down_plan=self._build_limit_down_plan(),
            override_rules=self._build_override_rules(),
            meta={"engine": "nextday_exit_v1", "version": "0.1.0"},
        )

    def decide_actions(self, plan: ExitPlan, prev_close: float, open_price: float, up_limit_price: float | None = None,
                       down_limit_price: float | None = None, override_event: Optional[Dict] = None) -> Dict:
        if override_event and override_event.get("enabled"):
            return {"state": "NEWS_OVERRIDE", "actions": override_event.get("actions", []), "reason": override_event.get("reason", "override")}
        state = classify_open_state(prev_close, open_price, up_limit_price, down_limit_price,
                                    self.config.high_open_threshold, self.config.low_open_threshold)
        mapping = {OPEN_HIGH: plan.high_open_plan, OPEN_FLAT: plan.flat_open_plan, OPEN_LOW: plan.low_open_plan,
                   LIMIT_UP: plan.limit_up_plan, LIMIT_DOWN: plan.limit_down_plan}
        return {"state": state, "actions": [x.to_dict() for x in mapping[state]], "reason": "plan_matched"}

    def _build_high_open_plan(self) -> List[ExitAction]:
        return [ExitAction("reduce", "高开后冲高承压，先减仓一半", self.config.first_take_profit_ratio, "market_or_best_limit", "优先兑现隔夜收益"),
                ExitAction("trail", "剩余仓位跟随盘中强弱处理", self.config.second_take_profit_ratio, "dynamic_trailing", "若强势延续则保留，转弱则卖出")]
    def _build_flat_open_plan(self) -> List[ExitAction]:
        return [ExitAction("observe", "平开先观察 10-30 分钟承接", 0.0, "no_order", "等待确认是否转强"),
                ExitAction("conditional_reduce", "若放量冲高失败则减仓", self.config.first_take_profit_ratio, "weakness_limit", "避免平开弱转弱")]
    def _build_low_open_plan(self) -> List[ExitAction]:
        return [ExitAction("reduce", "低开先减半控制风险", self.config.low_open_stop_ratio, "market_or_best_limit", "防止弱预期继续扩散"),
                ExitAction("observe", "剩余仓位观察是否修复", 0.0, "no_order", "若反抽失败则继续退出")]
    def _build_limit_up_plan(self) -> List[ExitAction]:
        return [ExitAction("hold", "涨停开盘继续观察封单质量", 0.0, "no_order", "不急于卖出，防止主升浪中过早兑现")]
    def _build_limit_down_plan(self) -> List[ExitAction]:
        return [ExitAction("queue_exit", "跌停则排队卖出，打开后立即执行", 1.0, "limit_down_queue_then_market", "优先保护本金")]
    def _build_override_rules(self) -> List[Dict]:
        return [{"rule_name": "major_positive_news_override", "desc": "若盘前或盘中出现重大正面新信息，可覆盖原卖出剧本"},
                {"rule_name": "major_negative_news_override", "desc": "若盘前或盘中出现重大负面新信息，可直接进入强制减仓或清仓逻辑"}]
