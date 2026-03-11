from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass
class ServiceHealthResult:
    service: str
    status: str
    checked_at: str
    detail: dict


class ServiceHealthChecker:
    def check_api(self) -> ServiceHealthResult:
        return ServiceHealthResult(
            service="api",
            status="ok",
            checked_at=datetime.utcnow().isoformat(),
            detail={"message": "api skeleton reachable"},
        )

    def check_database(self) -> ServiceHealthResult:
        return ServiceHealthResult(
            service="database",
            status="ok",
            checked_at=datetime.utcnow().isoformat(),
            detail={"message": "database connectivity not yet deep-checked"},
        )

    def check_policy(self) -> ServiceHealthResult:
        return ServiceHealthResult(
            service="policy",
            status="ok",
            checked_at=datetime.utcnow().isoformat(),
            detail={"message": "default policy available"},
        )

    def run_all(self) -> list[ServiceHealthResult]:
        return [
            self.check_api(),
            self.check_database(),
            self.check_policy(),
        ]
