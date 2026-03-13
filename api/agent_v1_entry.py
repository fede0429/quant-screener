from __future__ import annotations

"""A股 Agent V1 主线并入入口。

用途：
- 作为现有项目主线与 Agent V1 的桥接层
- 不直接改写原 server.py / learning_worker.py
- 先以组合装配方式并入
"""

from api.auction_executor import AuctionExecutor
from api.base_screener_adapter import BaseScreenerAdapter
from api.compliance_logger import ComplianceLogger
from api.kill_switch import KillSwitchManager
from api.manual_override import ManualOverrideManager
from api.nextday_exit_engine import NextDayExitEngine
from api.orchestrator import Orchestrator
from api.pipeline_stage1 import Stage1Pipeline
from api.risk_firewall import RiskFirewall
from api.tail_session_engine import TailSessionEngine


class AgentV1Entry:
    def __init__(self) -> None:
        self.base_screener_adapter = BaseScreenerAdapter()
        self.compliance_logger = ComplianceLogger()
        self.kill_switch = KillSwitchManager()
        self.manual_override = ManualOverrideManager()

        self.risk_firewall = RiskFirewall()
        self.orchestrator = Orchestrator(risk_firewall=self.risk_firewall)
        self.tail_session_engine = TailSessionEngine()
        self.auction_executor = AuctionExecutor()
        self.nextday_exit_engine = NextDayExitEngine()

        self.pipeline = Stage1Pipeline(
            orchestrator=self.orchestrator,
            tail_session_engine=self.tail_session_engine,
            auction_executor=self.auction_executor,
            nextday_exit_engine=self.nextday_exit_engine,
        )

    def build_from_base_candidates(
        self,
        trade_date: str,
        base_rows: list[dict],
        next_trade_date: str,
    ) -> dict:
        if self.kill_switch.is_blocking():
            return {
                "accepted": False,
                "reason": "kill_switch_blocking",
            }

        candidates = self.base_screener_adapter.normalize_candidates(base_rows)
        buckets = self.base_screener_adapter.split_buckets(candidates)

        results = []
        for candidate in buckets["core"]:
            decision_input = self.base_screener_adapter.to_decision_input(
                trade_date=trade_date,
                candidate=candidate,
                policy_score=0.0,
                event_score=0.0,
                technical_score=0.0,
                intl_adjustment=0.0,
                thesis="base_screener_only_bootstrap",
                reference_price=None,
                latest_price=None,
            )
            results.append(decision_input.to_dict())

        return {
            "accepted": True,
            "buckets": {
                "core": [x.to_dict() for x in buckets["core"]],
                "watch": [x.to_dict() for x in buckets["watch"]],
                "blocked": [x.to_dict() for x in buckets["blocked"]],
            },
            "decision_inputs": results,
            "next_trade_date": next_trade_date,
        }
