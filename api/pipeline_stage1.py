from __future__ import annotations

from typing import Dict, List, Optional

from api.announcement_ingestion import AnnouncementIngestion
from api.news_ingestion import NewsIngestion
from api.policy_ingestion import PolicyIngestion
from api.models.decision_input import DecisionInput
from api.nextday_exit_engine import NextDayExitEngine
from api.orchestrator import Orchestrator
from api.auction_executor import AuctionExecutor
from api.tail_session_engine import TailSessionEngine


class Stage1Pipeline:
    def __init__(self, orchestrator: Orchestrator, tail_session_engine: TailSessionEngine,
                 auction_executor: AuctionExecutor, nextday_exit_engine: NextDayExitEngine) -> None:
        self.orchestrator = orchestrator
        self.tail_session_engine = tail_session_engine
        self.auction_executor = auction_executor
        self.nextday_exit_engine = nextday_exit_engine
        self.policy_ingestion = PolicyIngestion()
        self.announcement_ingestion = AnnouncementIngestion()
        self.news_ingestion = NewsIngestion()

    def run_from_decision_input(self, item: DecisionInput, quantity: int, next_trade_date: str) -> Dict:
        decision = self.orchestrator.evaluate(item)
        if not decision["accepted"]:
            return {"accepted": False, "decision": decision, "order": None, "exit_plan": None}

        from api.models.trade_intent import TradeIntent

        intent = TradeIntent(**decision["intent"])
        intent = self.tail_session_engine.apply_tail_guard(intent, latest_price=item.latest_price)
        order = self.auction_executor.build_order(intent, quantity=quantity)
        exit_plan = self.nextday_exit_engine.build_exit_plan(intent.intent_id, intent.code, intent.trade_date, next_trade_date)

        return {
            "accepted": True,
            "decision": decision,
            "intent": intent.to_dict(),
            "order": {"executable": order.executable, "reason": order.reason, "order_payload": order.order_payload},
            "exit_plan": exit_plan.to_dict(),
        }

    def parse_events(self, policy_records: Optional[List[Dict]] = None, announcement_records: Optional[List[Dict]] = None,
                     news_records: Optional[List[Dict]] = None) -> Dict:
        return {
            "policy_events": [x.to_dict() for x in self.policy_ingestion.parse_records(policy_records or [])],
            "announcement_events": [x.to_dict() for x in self.announcement_ingestion.parse_records(announcement_records or [])],
            "news_events": [x.to_dict() for x in self.news_ingestion.parse_records(news_records or [])],
        }
