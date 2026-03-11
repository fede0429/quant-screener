from app.services.execution_service import ExecutionService


def test_execution_service_paper_flow():
    svc = ExecutionService()
    plan = svc.plan_paper_order(
        {"proposal_id": "p1", "symbol": "600519.SH", "side": "buy"},
        0.05,
    )
    order = svc.submit_paper_order(plan)
    fill = svc.simulate_fill(order)

    assert order["status"] == "submitted"
    assert fill["status"] == "filled"
    assert fill["symbol"] == "600519.SH"
