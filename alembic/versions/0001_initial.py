from alembic import op
import sqlalchemy as sa

revision = '0001_initial'
down_revision = None
branch_labels = None
depends_on = None

def upgrade():
    op.create_table(
        'research_reports',
        sa.Column('report_id', sa.String(), primary_key=True),
        sa.Column('symbol', sa.String(32), nullable=False),
        sa.Column('as_of_date', sa.Date(), nullable=False),
        sa.Column('report_type', sa.String(32), nullable=False),
        sa.Column('fundamental_score', sa.Float(), nullable=True),
        sa.Column('valuation_percentile', sa.Float(), nullable=True),
        sa.Column('decision_label', sa.String(32), nullable=True),
        sa.Column('confidence', sa.Float(), nullable=True),
        sa.Column('summary', sa.Text(), nullable=True),
        sa.Column('payload', sa.JSON(), nullable=False),
    )
    op.create_index('ix_research_reports_symbol', 'research_reports', ['symbol'])

    op.create_table(
        'portfolio_runs',
        sa.Column('run_id', sa.String(), primary_key=True),
        sa.Column('run_type', sa.String(32), nullable=False),
        sa.Column('strategy_name', sa.String(128), nullable=False),
        sa.Column('as_of_date', sa.Date(), nullable=False),
        sa.Column('config', sa.JSON(), nullable=False),
        sa.Column('summary', sa.JSON(), nullable=True),
    )

    op.create_table(
        'portfolio_holdings',
        sa.Column('id', sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column('run_id', sa.String(), nullable=False),
        sa.Column('symbol', sa.String(32), nullable=False),
        sa.Column('weight_target', sa.Float(), nullable=True),
        sa.Column('weight_actual', sa.Float(), nullable=True),
        sa.Column('score_source', sa.Float(), nullable=True),
        sa.Column('sector', sa.String(64), nullable=True),
        sa.Column('industry', sa.String(128), nullable=True),
        sa.Column('rebalance_action', sa.String(32), nullable=True),
    )

    op.create_table(
        'proposals',
        sa.Column('proposal_id', sa.String(), primary_key=True),
        sa.Column('strategy_name', sa.String(128), nullable=False),
        sa.Column('symbol', sa.String(32), nullable=False),
        sa.Column('side', sa.String(16), nullable=False),
        sa.Column('proposal_type', sa.String(32), nullable=False),
        sa.Column('as_of_time', sa.DateTime(), nullable=False),
        sa.Column('thesis', sa.Text(), nullable=False),
        sa.Column('entry_logic', sa.JSON(), nullable=False),
        sa.Column('invalidation_logic', sa.JSON(), nullable=False),
        sa.Column('stop_rule', sa.JSON(), nullable=True),
        sa.Column('target_rule', sa.JSON(), nullable=True),
        sa.Column('horizon_days', sa.String(16), nullable=True),
        sa.Column('confidence', sa.Float(), nullable=True),
        sa.Column('desired_weight', sa.Float(), nullable=True),
        sa.Column('max_weight', sa.Float(), nullable=True),
        sa.Column('urgency', sa.String(16), nullable=True),
        sa.Column('status', sa.String(32), nullable=False),
    )

    op.create_table(
        'risk_decisions',
        sa.Column('decision_id', sa.String(), primary_key=True),
        sa.Column('proposal_id', sa.String(), nullable=False),
        sa.Column('decision', sa.String(32), nullable=False),
        sa.Column('reason_codes', sa.JSON(), nullable=False),
        sa.Column('approved_weight', sa.Float(), nullable=True),
        sa.Column('risk_snapshot', sa.JSON(), nullable=False),
        sa.Column('market_state', sa.String(32), nullable=True),
        sa.Column('reviewer', sa.String(64), nullable=False),
    )

    op.create_table(
        'audit_events',
        sa.Column('event_id', sa.String(), primary_key=True),
        sa.Column('event_time', sa.DateTime(), nullable=False),
        sa.Column('event_type', sa.String(64), nullable=False),
        sa.Column('actor', sa.String(64), nullable=False),
        sa.Column('ref_type', sa.String(64), nullable=False),
        sa.Column('ref_id', sa.String(128), nullable=False),
        sa.Column('payload', sa.JSON(), nullable=False),
    )

def downgrade():
    op.drop_table('audit_events')
    op.drop_table('risk_decisions')
    op.drop_table('proposals')
    op.drop_table('portfolio_holdings')
    op.drop_table('portfolio_runs')
    op.drop_index('ix_research_reports_symbol', table_name='research_reports')
    op.drop_table('research_reports')
