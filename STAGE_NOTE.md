# 收口说明

## 这次做了什么
把原主线第一轮高优先级修复集中收口，避免你再单独推 4 个散包。

## 当前收口后的能力
### 已修
- 增量行情 `turnover` 写死为 0
- weekly 周频计算
- 历史回测模型前视偏差的注入式规避
- SQLite 连接配置
- 批量事务上下文
- 批量读取辅助接口
- server / worker 初始化重复问题

### 还没做
- 更深度的 `factor_engine.py` 优化
- `model_engine.py` 真正时点模型查询
- 更彻底的数据库访问层重构
- 真正生产级 FastAPI 路由收口

## 使用建议
这包适合先在：
`feature/mainline-repair-round1`
分支中整体并入。
