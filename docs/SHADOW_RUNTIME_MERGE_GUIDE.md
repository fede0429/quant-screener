# Shadow Learning 第一阶段并包指南

## 推荐顺序
直接以本收口包为准，不再回头逐个拼 3 个 Shadow Learning 散包。

## 建议步骤
1. 新建分支：
   `feature/shadow-runtime-round1`
2. 解压本包
3. 合并：
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
4. 做一次本地联调
5. 检查输出：
   - ledger snapshot
   - shadow replay result
   - outcome / comparison / review payload
   - suggestions / weight_delta / template_stats
6. 再合回主线

## 联调重点
### 提案与回放
- proposal 能入账
- shadow position 能生成
- replay 能输出 T+1 / T+3 / T+5 / MFE / MAE

### 复盘
- outcome_label 正常
- 真实单与影子单 diff 正常
- review_tags 能识别错失机会 / 错误接受 / 执行偏差

### 建议
- review payload 能产出 suggestions
- agent 权重建议正常
- template stats 能按模板汇总
