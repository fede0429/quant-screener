# Agent 真实接入总整合包联调清单

## 1. 关键文件
- `api/agent_v1_entry.py`
- `api/base_screener_runtime_bridge.py`
- `api/event_score_bridge.py`
- `api/technical_score_bridge.py`
- `api/agent_pipeline_runner.py`
- `api/compliance_logger.py`

## 2. 联调目标
### A. 三路输入
- Base Screener 真实输入
- Event Scores 真实输入
- Technical Scores 真实输入

### B. 主链路
- 进入 Stage1Pipeline
- 产出 TradeIntent
- 产出 Order Payload
- 产出 ExitPlan

### C. 合规
- Decision logs
- Order logs
- Risk logs

## 3. 建议下一步
这包并入后，下一步最合适的是：
- 开始修改真实 server / worker 挂接点
- 或做 broker API 接入包
