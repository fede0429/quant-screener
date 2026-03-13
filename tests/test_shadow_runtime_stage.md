# Shadow Learning 第一阶段收口包联调清单

## 1. 新增文件
- `api/models/shadow_models.py`
- `api/proposal_ledger.py`
- `api/shadow_position_store.py`
- `api/shadow_replay_engine.py`
- `api/shadow_runtime_service.py`
- `api/proposal_outcome_evaluator.py`
- `api/real_trade_shadow_comparator.py`
- `api/review_agent_hooks.py`
- `api/shadow_review_runtime.py`
- `api/suggestion_engine.py`
- `api/agent_weight_adjustor.py`
- `api/strategy_template_stats.py`
- `api/shadow_suggestion_runtime.py`

## 2. 核心检查
### A. 提案与头寸
- 所有 proposal 能入 ledger
- shadow position 能生成

### B. 回放与结果
- replay 能输出 t1/t3/t5 与 mfe/mae
- outcome evaluator 能输出 positive/negative/neutral

### C. 对比与复盘
- real trade vs shadow 能输出 diff
- review payload 能输出 tags

### D. 建议与统计
- suggestion engine 能输出建议
- agent 权重能输出 delta
- template stats 能汇总正负中性样本

## 3. 下一步建议
这包并入后，下一步最合适的是：
- 做 Shadow Learning 总整合包
- 或开始接知识系统 feedback hooks
