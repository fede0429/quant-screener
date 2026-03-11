from uuid import uuid4
from datetime import datetime
from app.models.research_report import ResearchReport
from app.models.audit_event import AuditEvent
from app.services.feature_service import FeatureService

class ResearchService:
    def __init__(self, db):
        self.db = db
        self.feature_service = FeatureService()

    def generate_fundamental_report(self, symbol, as_of_date):
        features = self.feature_service.compute_snapshot(symbol, as_of_date)
        growth_score = float(features.get("growth_score", 0.0))
        profitability_score = float(features.get("profitability_score", 0.0))
        solvency_score = float(features.get("solvency_score", 0.0))
        valuation_score = float(features.get("valuation_score", 0.0))
        valuation_percentile = float(features.get("valuation_percentile", 0.0))
        summary = "营收与净利润增长趋势较强" if growth_score >= 0.7 else "暂无显著亮点或风险"
        decision_label = "buy" if growth_score > 0.75 and profitability_score > 0.75 and valuation_score > 0.5 else "hold"
        report = ResearchReport(
            report_id=str(uuid4()),
            symbol=symbol,
            as_of_date=as_of_date,
            report_type="fundamental",
            fundamental_score=(growth_score + profitability_score + solvency_score + valuation_score) / 4,
            valuation_percentile=valuation_percentile,
            decision_label=decision_label,
            confidence=(growth_score + profitability_score) / 2,
            summary=summary,
            payload={"scores": features},
        )
        self.db.add(report)
        self.db.add(AuditEvent(
            event_id=str(uuid4()),
            event_time=datetime.utcnow(),
            event_type="research_report_created",
            actor="system",
            ref_type="research_report",
            ref_id=report.report_id,
            payload={"symbol": symbol, "report_type": "fundamental"},
        ))
        self.db.commit()
        self.db.refresh(report)
        return report
