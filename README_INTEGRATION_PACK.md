# Quant Platform v2 – Integration Pack

This pack is the next step after the adapter pack.

Included:
- data_service adapter skeleton
- execution_service skeleton for paper mode
- replay import helper
- end-to-end bootstrap script
- concrete integration notes for quant-screener
- basic tests

Recommended rollout:
1. Wire factor_engine into feature_service
2. Wire portfolio_lab / portfolio_advisor into portfolio_service
3. Wire model_engine into model_service
4. Use this pack to connect data + execution + replay
5. Keep everything in paper mode
