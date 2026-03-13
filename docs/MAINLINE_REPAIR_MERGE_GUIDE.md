# 原主线修复并包指南

## 推荐顺序
直接以本收口包为准，不再回头逐个拼 4 个修复散包。

## 建议步骤
1. 新建分支：
   `feature/mainline-repair-round1`
2. 解压本包
3. 合并：
   - `api/data_fetcher.py`
   - `api/backtest_engine.py`
   - `api/cache_db.py`
   - `api/app_context.py`
   - `api/server.py`
   - `api/learning_worker.py`
4. 做一次本地联调
5. 做一次回测验证
6. 再合回主线

## 联调重点
### 数据层
- 增量刷新后 turnover 不再统一为 0
- transaction 上下文可用
- 批量读取接口可用

### 回测层
- weekly 周期合理
- `historical_model_lookup` 不报错
- 不传 lookup 也兼容

### 编排层
- server 和 worker 都走 `build_app_context()`
- 如果不存在 Agent V1，也能正常启动
