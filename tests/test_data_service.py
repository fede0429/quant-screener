from datetime import date
from app.services.data_service import DataService


def test_data_service_returns_snapshot():
    svc = DataService()
    result = svc.get_daily_snapshot("600519.SH", date.today())
    assert isinstance(result, dict)
    assert result["symbol"] == "600519.SH"
