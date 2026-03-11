from app.observability.config_validator import ConfigValidator

def test_config_validator(tmp_path):
    path = tmp_path / "policy.yaml"
    path.write_text("mode: paper\nmax_position_weight: 0.08\nmax_sector_weight: 0.25\n", encoding="utf-8")
    result = ConfigValidator().validate(str(path), "policy")
    assert result["ok"] is True
    assert result["missing_keys"] == []
