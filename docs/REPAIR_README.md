# 原主线修复收口包

这是把前面 4 个原主线修复优化包统一整理后的 **阶段收口包**，用于更方便地直接并入你现有 `quant-screener` 主线。

## 已收口内容
1. `data_fetcher.py` 增量行情质量修复
2. `backtest_engine.py` weekly 周期 + 模型前视修复
3. `cache_db.py` 连接模型 + 事务 + 批量读取修复
4. `server.py / learning_worker.py / app_context.py` 初始化收口

## 本包定位
- 这是 **原主线修复阶段收口包**
- 适合直接作为主线修复基线
- 建议优先并入功能分支，再合回主线

## 包内重点
- `docs/MAINLINE_REPAIR_MERGE_GUIDE.md`
- `docs/REPAIR_ORDER.md`
- `api/data_fetcher.py`
- `api/backtest_engine.py`
- `api/cache_db.py`
- `api/app_context.py`
- `api/server.py`
- `api/learning_worker.py`
