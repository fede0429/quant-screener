# 主线修复收口包联调清单

## 1. 替换文件
- `api/data_fetcher.py`
- `api/backtest_engine.py`
- `api/cache_db.py`
- `api/app_context.py`
- `api/server.py`
- `api/learning_worker.py`

## 2. 核心检查
### A. 数据刷新
- 增量刷新正常
- turnover 不再被统一写成 0

### B. 回测
- weekly 回测日期合理
- historical_model_lookup 可注入

### C. 数据库
- `transaction()` 正常
- `get_prices_for_codes()` 等接口正常

### D. 编排
- server 正常启动
- worker 正常启动
- AppContext 装配一致

## 3. 建议下一步
主线修复收口后，可以回到：
- Agent 主线真实接入
- 或继续做主线第二轮优化
