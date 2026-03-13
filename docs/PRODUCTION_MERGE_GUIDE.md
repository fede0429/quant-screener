# 生产闭环并包指南

## 推荐顺序
直接以本收口包为准，不再回头逐个拼 4 个生产散包。

## 建议步骤
1. 新建分支：
   `feature/production-round1`
2. 解压本包
3. 合并：
   - `api/source_registry.py`
   - `api/source_clients.py`
   - `api/source_fetch_runner.py`
   - `api/broker_adapter.py`
   - `api/order_execution_service.py`
   - `api/runtime_scheduler.py`
   - `api/runtime_jobs.py`
   - `api/runtime_monitor.py`
   - `api/alert_router.py`
4. 做一次本地 dry-run 联调
5. 再逐步替换真实实现
6. 再合回主线

## 联调重点
### 数据源
- source registry 正常
- fetch runner 能跑

### 执行
- order execution service 能接 order payload
- broker adapter 默认 dry-run

### 编排
- runtime scheduler 能统一调用 jobs

### 可观测性
- runtime monitor 能记录
- alert router 接口固定
