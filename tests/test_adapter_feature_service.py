from datetime import date
from app.services.feature_service import FeatureService

def test_feature_service_returns_dict():
    svc = FeatureService()
    result = svc.compute_snapshot("600519.SH", date.today())
    assert isinstance(result, dict)
    assert "growth_score" in result
