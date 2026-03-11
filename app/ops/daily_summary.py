from __future__ import annotations

from datetime import datetime


class DailySummaryBuilder:
    def build(
        self,
        run_id: str,
        reports_count: int,
        proposals_count: int,
        approved_count: int,
        rejected_count: int,
        fills_count: int,
    ) -> dict:
        return {
            "run_id": run_id,
            "generated_at": datetime.utcnow().isoformat(),
            "reports_count": reports_count,
            "proposals_count": proposals_count,
            "approved_count": approved_count,
            "rejected_count": rejected_count,
            "fills_count": fills_count,
            "status": "paper_complete",
        }
