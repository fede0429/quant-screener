from app.observability.metrics import MetricsCollector

def test_metrics_collector():
    m = MetricsCollector()
    m.inc("reports")
    m.inc("reports", 2)
    m.set("fills", 1)
    snap = m.snapshot()
    assert snap["reports"] == 3
    assert snap["fills"] == 1
