# 整体收口说明

## 这次整体收口的目标
把以下内容统一收口：

- A股 Agent V1 主线并入桥接能力
- Agent 主线真实接入三条输入链
- Stage1Pipeline 主链路
- 合规日志 / 人工干预 / Kill Switch
- 运行入口与桥接层

## 已收口内容
### 主线桥接
- `agent_v1_entry.py`
- `base_screener_adapter.py`
- `pipeline_stage1.py`

### 真实输入
- `base_screener_runtime_bridge.py`
- `event_score_aggregator.py`
- `event_score_bridge.py`
- `technical_score_aggregator.py`
- `technical_score_bridge.py`

### 合规与人工接管
- `compliance_logger.py`
- `manual_override.py`
- `kill_switch.py`

## 下一阶段建议
1. 把 bridge/runners 挂到真实 `server.py` / `learning_worker.py`
2. 接真实消息源抓取器
3. 接真实技术确认特征计算
4. 接真实 broker API
5. 做联调与回放
