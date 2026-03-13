# 主线修复顺序说明

如果你还是希望分段推送，顺序如下：

1. `api/data_fetcher.py`
2. `api/backtest_engine.py`
3. `api/cache_db.py`
4. `api/app_context.py`
5. `api/server.py`
6. `api/learning_worker.py`

如果想省事，直接推本收口包即可。
