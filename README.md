# Quant Platform v2 Unified Baseline

This is a unified baseline that consolidates the earlier scaffold/pack ideas into one clean starter.

Included:
- FastAPI app skeleton
- SQLAlchemy models
- Pydantic schemas
- Research / Proposal / Risk / Replay routes
- Services for research, proposal generation, risk evaluation
- Governance helpers: state machine, policy engine, execution mode guard
- Alembic scaffold with a single initial migration
- Docker / docker-compose
- Demo seed script
- Demo pipeline runner
- Basic tests
- MIGRATION_GAP_REPORT.md

Goal:
Provide one clean base for migrating the existing quant-screener research stack into
a controlled proposal/risk/paper-trading architecture.
