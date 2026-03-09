# Quant Screener

This project now includes a first-phase daily learning loop for potential-stock selection.

## What Was Added

- Daily feature snapshots for the whole screened universe
- Future return labels for 5/20/60 trading-day horizons
- Online weight learning based on realized alpha
- Daily review report generation
- A standalone scheduler worker for automatic end-of-day review

## New API Endpoints

- `GET /api/learning/status`
- `GET /api/learning/weights`
- `GET /api/learning/report/latest`
- `POST /api/learning/run`
- `POST /api/learning/bootstrap`
- `POST /api/data/indicator-history/backfill`

Example request:

```json
{
  "top_n": 20,
  "label_horizons": [5, 20, 60],
  "learning_horizon": 20,
  "lookback_runs": 60,
  "min_labeled_rows": 200,
  "auto_apply": true,
  "refresh_before_run": false
}
```

## Daily Worker

The worker entrypoint is:

```bash
python /app/api/learning_worker.py
```

Default environment settings:

- `LEARNING_TIMEZONE=Asia/Shanghai`
- `LEARNING_RUN_HOUR=18`
- `LEARNING_RUN_MINUTE=10`
- `LEARNING_TOP_N=20`
- `LEARNING_HORIZON=20`
- `LEARNING_LABEL_HORIZONS=5,20,60`
- `LEARNING_LOOKBACK_RUNS=60`
- `LEARNING_MIN_LABELED_ROWS=200`
- `LEARNING_AUTO_APPLY=true`
- `LEARNING_REFRESH_BEFORE_RUN=true`

## Phase 1 Scope

This phase does not introduce a full ML ranker yet. It builds the daily learning loop first:

1. Save daily screener snapshots
2. Backfill realized labels when the holding window matures
3. Measure which factor groups actually delivered alpha
4. Update factor weights for the next daily run

The next phase should focus on point-in-time backtesting and a proper model trainer.

## Phase 2 Scope

Phase 2 introduces point-in-time replay:

1. Historical `daily_basic` snapshots are stored in `indicator_history`
2. Financial reports can use `ann_date` and `f_ann_date` when available
3. Backtests rebuild the stock universe at each rebalance date
4. Bootstrap snapshots use the same point-in-time builder as backtests

## Phase 3 Scope

Phase 3 introduced a first ranking model on top of the point-in-time dataset:

1. `feature_snapshots` and `learning_labels` can be exported as training rows
2. A walk-forward `RandomForestRegressor` is trained against horizon alpha
3. Trained models are tracked in `model_registry`
4. Latest predictions are stored in `model_predictions`

## Phase 3 API Endpoints

- `POST /api/model/train`
- `GET /api/model/latest`

Phase 3 now also adds:

- default risk filtering for `ST`, suspended, low-liquidity, and micro-cap names
- promotion gates so a weak model is not automatically activated
- optional `model_score` blending in `POST /api/screener`

## Phase 4 Scope

Phase 4 upgrades the model trainer from pure alpha regression to a stock-picking classifier:

1. `POST /api/model/train` now supports `task_type=classification`
2. Labels can be built from `alpha_top_quantile`, `alpha_positive`, or `alpha_threshold`
3. A walk-forward `HistGradientBoostingClassifier` scores the cross-section with probabilities
4. Validation now tracks `precision@20`, `top20 alpha`, `top20 hit rate`, and `rank_ic`
5. Promotion gates can block weak classifiers before they enter serving

Example training request:

```json
{
  "horizon_days": 20,
  "task_type": "classification",
  "validation_runs": 1,
  "min_train_runs": 2,
  "min_rows": 1000,
  "label_mode": "alpha_top_quantile",
  "label_quantile": 0.2,
  "score_latest_snapshot": true,
  "latest_limit": 20,
  "activate": true
}
```
## Phase 5 Scope

Phase 5 adds a portfolio-construction and monitoring layer on top of the live screener:

1. `POST /api/screener` can now return a constrained portfolio preview
2. Portfolio construction supports sector or industry neutralization by round-robin selection
3. Position, sector, and industry caps are enforced before weights are assigned
4. `GET /api/model/monitor/latest` returns the active model status, live score distribution, and exposure summary

Example screener request:

```json
{
  "use_model": true,
  "model_horizon": 20,
  "model_weight": 0.35,
  "build_portfolio": true,
  "portfolio_top_n": 20,
  "neutralize_by": "sector",
  "max_position_weight": 0.05,
  "max_sector_weight": 0.25,
  "max_industry_weight": 0.15,
  "max_positions_per_sector": 4,
  "max_positions_per_industry": 2
}
```

## Phase 6 Scope

Phase 6 extends the stack from live ranking into portfolio simulation and monitoring archives:

1. `POST /api/portfolio/backtest` runs point-in-time portfolio backtests with model blending, portfolio constraints, turnover, and transaction-cost simulation
2. Backtest outputs now include gross/net curves, turnover, transaction cost, and per-period holdings history
3. `GET /api/model/monitor/latest` can persist the latest live monitor snapshot into `model_monitor_reports`
4. `GET /api/model/monitor/history` returns archived monitor reports for historical review
5. The daily learning worker now auto-archives a monitor report after each learning cycle by default

Example portfolio backtest request:

```json
{
  "frequency": "weekly",
  "top_n": 40,
  "portfolio_top_n": 20,
  "use_model": true,
  "model_horizon": 20,
  "model_weight": 0.35,
  "neutralize_by": "sector",
  "max_position_weight": 0.05,
  "max_sector_weight": 0.25,
  "max_industry_weight": 0.15,
  "max_positions_per_sector": 4,
  "max_positions_per_industry": 2,
  "transaction_cost_bps": 10.0
}
```

## Phase 7 Scope

Phase 7 turns the portfolio layer into a research loop:

1. `POST /api/portfolio/sweep` scans parameter grids and ranks combinations by alpha, sharpe, or turnover-aware scores
2. Portfolio construction now supports turnover-aware rebalancing with `current_holdings`, `rebalance_buffer`, `max_new_positions`, and `min_holding_periods`
3. `POST /api/portfolio/backtest` now simulates these rebalance rules and reports average name turnover plus average new positions
4. `GET /api/model/monitor/dashboard` aggregates archived monitor reports into a historical dashboard with turnover and concentration trends
5. `POST /api/screener` can preview a lower-turnover rebalance plan when you pass current holdings

Example parameter sweep request:

```json
{
  "start_date": "2025-10-01",
  "end_date": "2026-03-06",
  "optimize_for": "alpha_turnover",
  "top_results": 5,
  "max_combinations": 20,
  "frequency_values": ["monthly"],
  "top_n_values": [30, 40],
  "portfolio_top_n_values": [15, 20],
  "rebalance_buffer_values": [0, 5],
  "max_new_positions_values": [4, 8],
  "min_holding_periods_values": [0, 1]
}
```

## Phase 8 Scope

Phase 8 promotes the research loop into an automated portfolio-advisor layer:

1. `POST /api/portfolio/profile/optimize` runs a sweep, stores the winning parameter set as a portfolio profile, and can auto-activate it when it beats the current profile
2. `GET /api/portfolio/profile/active` and `GET /api/portfolio/profile/history` expose the active and historical optimization profiles
3. `POST /api/portfolio/signal/run` generates a daily portfolio signal from the active profile and archives it into `portfolio_signal_reports`
4. `GET /api/portfolio/signal/latest`, `GET /api/portfolio/signal/history`, and `GET /api/portfolio/report/latest` expose the archived signal stream and daily rebalance report
5. The daily worker now supports automatic profile optimization and automatic signal/report archival

Example profile optimization request:

```json
{
  "optimize_for": "alpha_turnover",
  "top_results": 5,
  "max_combinations": 20,
  "min_improvement": 0.25,
  "frequency_values": ["monthly"],
  "top_n_values": [30, 40],
  "portfolio_top_n_values": [15, 20],
  "rebalance_buffer_values": [0, 5],
  "max_new_positions_values": [4, 8],
  "min_holding_periods_values": [0, 1]
}
```
