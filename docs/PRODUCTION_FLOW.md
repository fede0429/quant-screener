# 生产闭环流程

真实消息源
-> SourceRegistry / SourceClients
-> SourceFetchRunner

交易执行
-> BrokerAdapter
-> OrderExecutionService

运行编排
-> RuntimeJobs
-> RuntimeScheduler

监控与告警
-> RuntimeMonitor
-> AlertRouter

后续与 Agent 主链路对接：
- SourceFetchRunner 产出事件输入
- OrderExecutionService 消费 order payload
- RuntimeScheduler 驱动 preopen / intraday / tail-session / nextday-exit
- RuntimeMonitor / AlertRouter 覆盖全链路
