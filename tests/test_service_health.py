from app.ops.service_health import ServiceHealthChecker


def test_service_health_checker():
    checker = ServiceHealthChecker()
    results = checker.run_all()
    assert len(results) >= 3
    assert all(r.status == "ok" for r in results)
