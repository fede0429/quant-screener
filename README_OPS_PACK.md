# Quant Platform v2 – Ops Pack

This pack extends the unified baseline with operational tooling for paper-mode rollout.

Included:
- scheduler worker skeleton
- health/service status checker
- daily summary report builder
- risk policy loader
- simple strategy registry bootstrap
- ops runbook
- basic tests

Recommended order:
1. Keep research/proposal/risk chain in paper mode
2. Add scheduler for EOD runs
3. Add daily summary outputs
4. Add service health checks
5. Only then consider any live-mode review
