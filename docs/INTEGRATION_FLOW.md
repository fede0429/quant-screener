# 总整合流程

Base Screener 真实输出
-> BaseScreenerAdapter
-> BaseScreenerRuntimeBridge

真实事件输入
-> EventScoreAggregator
-> EventScoreBridge

真实技术确认输入
-> TechnicalScoreAggregator
-> TechnicalScoreBridge

三路分数合并
-> DecisionInput
-> Stage1Pipeline
-> Orchestrator / RiskFirewall
-> TradeIntent
-> AuctionExecutor
-> ExitPlan
-> ComplianceLogger
