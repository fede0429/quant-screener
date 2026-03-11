# Ops Runbook

## Daily paper-mode checklist
1. Run health checks
2. Sync/import latest research inputs
3. Generate research reports
4. Import or build portfolio preview
5. Generate proposals
6. Evaluate risk
7. Execute paper orders
8. Persist replay trail
9. Generate daily summary report

## Incident handling
- If data adapter fails: stop before proposal generation
- If risk engine errors: reject all pending proposals by default
- If replay logging fails: do not promote beyond paper mode
- If market_state=stress: degrade or skip new paper orders

## Promotion rule
No live execution until:
- paper chain is stable
- replay coverage is complete
- risk policies are versioned
- strategy whitelist is explicit
