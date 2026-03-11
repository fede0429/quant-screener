from pathlib import Path

class ConfigValidator:
    REQUIRED_KEYS = {
        "policy": {"mode", "max_position_weight", "max_sector_weight"},
        "strategy": {"strategy_name", "universe", "top_n"},
    }

    def _parse_simple_yaml(self, path: str) -> dict:
        data = {}
        for raw in Path(path).read_text(encoding="utf-8").splitlines():
            line = raw.strip()
            if not line or line.startswith("#") or ":" not in line:
                continue
            key, value = line.split(":", 1)
            data[key.strip()] = value.strip()
        return data

    def validate(self, path: str, config_type: str) -> dict:
        data = self._parse_simple_yaml(path)
        required = self.REQUIRED_KEYS[config_type]
        missing = sorted(k for k in required if k not in data)
        return {
            "path": path,
            "config_type": config_type,
            "ok": len(missing) == 0,
            "missing_keys": missing,
            "data": data,
        }
