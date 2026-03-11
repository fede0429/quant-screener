from app.core.policy_engine import PolicyEngine

def test_policy_engine_caps_weight():
    engine = PolicyEngine({"max_position_weight": 0.08, "max_sector_weight": 0.25})
    result = engine.evaluate(
        {"desired_weight": 0.10},
        {"max_position_weight": 0.08, "max_sector_weight": 0.25, "sector_exposure": {"食品饮料": 0.10}},
        {"proposal_sector": "食品饮料", "liquidity_ok": True, "market_state": "normal"},
    )
    assert result.approved_weight <= 0.08
