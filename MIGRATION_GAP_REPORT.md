# Migration Gap Report

## What to keep from quant-screener
- data_fetcher.py -> adapter into app/services/data_service.py
- factor_engine.py -> adapter into app/services/feature_service.py
- portfolio_lab.py / portfolio_advisor.py -> adapter into app/services/portfolio_service.py
- model_engine.py -> adapter into app/services/model_service.py
- monitor_engine.py -> selectively migrate into audit/replay logic

## What to build new in v2
- proposal_service.py
- risk_service.py
- replay route and audit event flow
- policy engine and state machine
- execution mode guard
- paper execution layer

## Immediate repository actions
1. Rotate and remove any exposed Tushare token from public history.
2. Keep current repo as research-core during transition.
3. Add this baseline under a new app/ tree.
4. Migrate endpoints incrementally under /api/v2.
5. Stay in paper mode until audit and replay are stable.
