from app.services.model_service import ModelService

def test_model_service_returns_prediction():
    svc = ModelService()
    result = svc.get_latest_prediction("600519.SH", "2026-03-11")
    assert isinstance(result, dict)
    assert result["symbol"] == "600519.SH"
