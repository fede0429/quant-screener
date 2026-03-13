# Shadow Learning 第一阶段流程

Strategy Proposal
-> ProposalLedger
-> ShadowPositionStore
-> ShadowReplayEngine

Shadow Replay Result
-> ProposalOutcomeEvaluator

Real Trade (optional)
-> RealTradeVsShadowComparator

Review Payload
-> ReviewAgentHooks
-> SuggestionEngine
-> AgentWeightAdjustor
-> StrategyTemplateStats

Runtime Assembly
-> ShadowRuntimeService
-> ShadowReviewRuntime
-> ShadowSuggestionRuntime
