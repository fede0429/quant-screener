from pathlib import Path
from app.ops.policy_loader import PolicyLoader


def test_policy_loader(tmp_path):
    path = tmp_path / "policy.yaml"
    path.write_text("max_position_weight: 0.08\nmode: paper\n", encoding="utf-8")
    result = PolicyLoader().load_simple_yaml(str(path))
    assert result["mode"] == "paper"
