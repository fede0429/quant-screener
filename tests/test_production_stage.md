# 生产闭环收口包联调清单

## 1. 新增文件
- `api/source_registry.py`
- `api/source_clients.py`
- `api/source_fetch_runner.py`
- `api/broker_adapter.py`
- `api/order_execution_service.py`
- `api/runtime_jobs.py`
- `api/runtime_scheduler.py`
- `api/runtime_monitor.py`
- `api/alert_router.py`

## 2. 核心检查
### A. Source runner
- 能列出启用源
- run_once 不报错

### B. Broker execution
- adapter 接口齐全
- execution service 能接 order payload

### C. Runtime orchestration
- scheduler 能统一跑作业

### D. Monitoring / alerting
- monitor 能记录事件
- alert router 接口固定

## 3. 下一步建议
这包并入后，下一步最合适的是：
- 做“生产闭环总整合包”
- 或开始把这些模块挂到真实 server / worker
