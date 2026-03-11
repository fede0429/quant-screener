from __future__ import annotations

from pathlib import Path


class PolicyLoader:
    def load_simple_yaml(self, path: str) -> dict:
        data = {}
        file_path = Path(path)
        if not file_path.exists():
            raise FileNotFoundError(path)

        for raw_line in file_path.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            if ":" not in line:
                continue
            key, value = line.split(":", 1)
            data[key.strip()] = value.strip()
        return data
