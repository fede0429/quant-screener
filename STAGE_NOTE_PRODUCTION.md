# 收口说明

## 这次做了什么
把生产闭环方向上的 4 个散包统一收口，避免你再逐个推进。

## 当前收口后的能力
### 已具备骨架
- 真实消息源注册与抓取 runner
- 真实 broker adapter 与执行 service
- 统一 runtime scheduler / jobs
- 运行监控与告警 router

### 还没做
- 真实 source clients 抓取实现
- 真实 broker API 实现
- worker / server 正式挂接
- webhook / email / IM 告警实现
- 全链路持久化

## 使用建议
这包适合先在：
`feature/production-round1`
分支中整体并入。
