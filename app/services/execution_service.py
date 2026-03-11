from __future__ import annotations

from uuid import uuid4
from datetime import datetime


class ExecutionService:
    def __init__(self):
        self.orders = []
        self.fills = []

    def plan_paper_order(self, proposal: dict, approved_weight: float) -> dict:
        return {
            "order_plan_id": str(uuid4()),
            "proposal_id": proposal["proposal_id"],
            "symbol": proposal["symbol"],
            "side": proposal["side"],
            "approved_weight": approved_weight,
            "mode": "paper",
            "status": "planned",
            "created_at": datetime.utcnow().isoformat(),
        }

    def submit_paper_order(self, order_plan: dict) -> dict:
        order = dict(order_plan)
        order["status"] = "submitted"
        self.orders.append(order)
        return order

    def simulate_fill(self, order: dict) -> dict:
        fill = {
            "fill_id": str(uuid4()),
            "order_plan_id": order["order_plan_id"],
            "symbol": order["symbol"],
            "side": order["side"],
            "status": "filled",
            "filled_at": datetime.utcnow().isoformat(),
        }
        self.fills.append(fill)
        return fill
