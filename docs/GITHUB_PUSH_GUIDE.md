# GitHub 推进建议

## 当前建议
如果你准备把 Agent 第一轮真实接入推进到 GitHub，优先用本总整合包。

## 建议分支
`feature/agent-runtime-final-round1`

## 推荐推进顺序
1. 先推 `api/models/` 与 `api/utils/`
2. 再推桥接层：
   - `base_screener_adapter.py`
   - `base_screener_runtime_bridge.py`
   - `event_score_aggregator.py`
   - `event_score_bridge.py`
   - `technical_score_aggregator.py`
   - `technical_score_bridge.py`
3. 再推主链路：
   - `pipeline_stage1.py`
   - `agent_pipeline_runner.py`
   - `agent_v1_entry.py`
4. 再推合规层：
   - `compliance_logger.py`
   - `manual_override.py`
   - `kill_switch.py`
5. 最后推 docs / config / tests

## 最省事方式
如果你不想拆太细，直接把本包整体并入功能分支即可。
