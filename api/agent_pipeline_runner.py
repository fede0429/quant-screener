from __future__ import annotations

"""
Agent 主线运行入口示例

作用：
- 组合 BaseScreenerRuntimeBridge
- 统一创建 pipeline / logger / adapter
"""

from api.auction_executor import AuctionExecutor
from api.base_screener_runtime_bridge import BaseScreenerRuntimeBridge
from api.compliance_logger import ComplianceLogger
from api.nextday_exit_engine import NextDayExitEngine
from api.orchestrator import Orchestrator
from api.risk_firewall import RiskFirewall
from api.tail_session_engine import TailSessionEngine
from api.pipeline_stage1 import Stage1Pipeline


def build_runtime_bridge():
    compliance_logger = ComplianceLogger()
    pipeline = Stage1Pipeline(
        orchestrator=Orchestrator(risk_firewall=RiskFirewall()),
        tail_session_engine=TailSessionEngine(),
        auction_executor=AuctionExecutor(),
        nextday_exit_engine=NextDayExitEngine(),
    )
    bridge = BaseScreenerRuntimeBridge(
        pipeline=pipeline,
        compliance_logger=compliance_logger,
    )
    return bridge, compliance_logger
