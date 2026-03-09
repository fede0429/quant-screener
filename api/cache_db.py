"""
SQLite cache and learning storage.
"""
import json
import logging
import sqlite3
from datetime import datetime

logger = logging.getLogger(__name__)


class CacheDB:
    def __init__(self, db_path: str):
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        self._create_tables()
        logger.info("SQLite cache database opened: %s", db_path)

    def _create_tables(self):
        self.conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT,
                updated_at TEXT DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS stocks (
                code TEXT PRIMARY KEY,
                name TEXT,
                industry TEXT,
                sector TEXT,
                area TEXT,
                market TEXT,
                list_date TEXT,
                extra_json TEXT,
                updated_at TEXT DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS daily_prices (
                code TEXT,
                trade_date TEXT,
                open REAL,
                high REAL,
                low REAL,
                close REAL,
                volume REAL,
                amount REAL,
                pct_chg REAL,
                turnover REAL,
                PRIMARY KEY (code, trade_date)
            );

            CREATE TABLE IF NOT EXISTS financials (
                code TEXT,
                end_date TEXT,
                ann_date TEXT,
                f_ann_date TEXT,
                revenue REAL,
                net_income REAL,
                gross_profit REAL,
                total_assets REAL,
                total_liabilities REAL,
                shareholders_equity REAL,
                operating_cash_flow REAL,
                free_cash_flow REAL,
                eps REAL,
                bps REAL,
                roe REAL,
                extra_json TEXT,
                PRIMARY KEY (code, end_date)
            );

            CREATE TABLE IF NOT EXISTS indicators (
                code TEXT PRIMARY KEY,
                pe REAL,
                pe_ttm REAL,
                pb REAL,
                ps REAL,
                ps_ttm REAL,
                dv_ratio REAL,
                dv_ttm REAL,
                total_mv REAL,
                circ_mv REAL,
                updated_at TEXT DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS indicator_history (
                code TEXT,
                trade_date TEXT,
                pe REAL,
                pe_ttm REAL,
                pb REAL,
                ps REAL,
                ps_ttm REAL,
                dv_ratio REAL,
                dv_ttm REAL,
                total_mv REAL,
                circ_mv REAL,
                created_at TEXT DEFAULT (datetime('now')),
                PRIMARY KEY (code, trade_date)
            );

            CREATE TABLE IF NOT EXISTS benchmark_prices (
                code TEXT,
                trade_date TEXT,
                open REAL,
                high REAL,
                low REAL,
                close REAL,
                volume REAL,
                amount REAL,
                pct_chg REAL,
                PRIMARY KEY (code, trade_date)
            );

            CREATE TABLE IF NOT EXISTS feature_snapshots (
                snapshot_date TEXT,
                code TEXT,
                name TEXT,
                industry TEXT,
                sector TEXT,
                rank INTEGER,
                composite_score REAL,
                value_score REAL,
                growth_score REAL,
                quality_score REAL,
                momentum_score REAL,
                price REAL,
                pe REAL,
                pb REAL,
                roe REAL,
                revenue_growth REAL,
                net_income_growth REAL,
                momentum_20d REAL,
                momentum_60d REAL,
                weights_json TEXT,
                filters_json TEXT,
                sectors_json TEXT,
                extra_json TEXT,
                created_at TEXT DEFAULT (datetime('now')),
                PRIMARY KEY (snapshot_date, code)
            );

            CREATE TABLE IF NOT EXISTS learning_labels (
                snapshot_date TEXT,
                code TEXT,
                horizon_days INTEGER,
                entry_date TEXT,
                exit_date TEXT,
                entry_price REAL,
                exit_price REAL,
                stock_return REAL,
                benchmark_return REAL,
                alpha REAL,
                created_at TEXT DEFAULT (datetime('now')),
                PRIMARY KEY (snapshot_date, code, horizon_days)
            );

            CREATE TABLE IF NOT EXISTS learning_runs (
                run_date TEXT PRIMARY KEY,
                top_n INTEGER,
                total_candidates INTEGER,
                snapshot_count INTEGER,
                active_weights_json TEXT,
                learned_weights_json TEXT,
                label_stats_json TEXT,
                meta_json TEXT,
                created_at TEXT DEFAULT (datetime('now')),
                updated_at TEXT DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS review_reports (
                report_date TEXT PRIMARY KEY,
                learning_horizon INTEGER,
                summary_md TEXT,
                stats_json TEXT,
                created_at TEXT DEFAULT (datetime('now')),
                updated_at TEXT DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS model_registry (
                model_id TEXT PRIMARY KEY,
                model_type TEXT,
                horizon_days INTEGER,
                train_start_date TEXT,
                train_end_date TEXT,
                validation_start_date TEXT,
                validation_end_date TEXT,
                feature_names_json TEXT,
                metrics_json TEXT,
                artifact_path TEXT,
                train_rows INTEGER,
                validation_rows INTEGER,
                is_active INTEGER DEFAULT 0,
                extra_json TEXT,
                created_at TEXT DEFAULT (datetime('now')),
                updated_at TEXT DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS model_predictions (
                model_id TEXT,
                snapshot_date TEXT,
                code TEXT,
                name TEXT,
                sector TEXT,
                baseline_rank INTEGER,
                composite_score REAL,
                model_score REAL,
                model_rank INTEGER,
                created_at TEXT DEFAULT (datetime('now')),
                PRIMARY KEY (model_id, snapshot_date, code)
            );

            CREATE TABLE IF NOT EXISTS model_monitor_reports (
                report_date TEXT,
                model_id TEXT,
                horizon_days INTEGER,
                summary_md TEXT,
                stats_json TEXT,
                created_at TEXT DEFAULT (datetime('now')),
                updated_at TEXT DEFAULT (datetime('now')),
                PRIMARY KEY (report_date, model_id)
            );

            CREATE TABLE IF NOT EXISTS portfolio_profiles (
                profile_id TEXT PRIMARY KEY,
                name TEXT,
                optimize_for TEXT,
                config_json TEXT,
                metrics_json TEXT,
                extra_json TEXT,
                is_active INTEGER DEFAULT 0,
                created_at TEXT DEFAULT (datetime('now')),
                updated_at TEXT DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS portfolio_signal_reports (
                signal_date TEXT,
                profile_id TEXT,
                model_id TEXT,
                summary_md TEXT,
                stats_json TEXT,
                created_at TEXT DEFAULT (datetime('now')),
                updated_at TEXT DEFAULT (datetime('now')),
                PRIMARY KEY (signal_date, profile_id)
            );

            CREATE INDEX IF NOT EXISTS idx_prices_code ON daily_prices(code);
            CREATE INDEX IF NOT EXISTS idx_prices_date ON daily_prices(trade_date);
            CREATE INDEX IF NOT EXISTS idx_fin_code ON financials(code);
            CREATE INDEX IF NOT EXISTS idx_indicator_hist_code_date ON indicator_history(code, trade_date);
            CREATE INDEX IF NOT EXISTS idx_indicator_hist_date ON indicator_history(trade_date);
            CREATE INDEX IF NOT EXISTS idx_bench_date ON benchmark_prices(trade_date);
            CREATE INDEX IF NOT EXISTS idx_snapshots_date ON feature_snapshots(snapshot_date);
            CREATE INDEX IF NOT EXISTS idx_snapshots_rank ON feature_snapshots(snapshot_date, rank);
            CREATE INDEX IF NOT EXISTS idx_labels_horizon ON learning_labels(horizon_days, snapshot_date);
            CREATE INDEX IF NOT EXISTS idx_model_registry_active ON model_registry(is_active, created_at);
            CREATE INDEX IF NOT EXISTS idx_model_predictions_snapshot ON model_predictions(snapshot_date, model_rank);
            CREATE INDEX IF NOT EXISTS idx_model_monitor_reports_date ON model_monitor_reports(report_date DESC, model_id);
            CREATE INDEX IF NOT EXISTS idx_portfolio_profiles_active ON portfolio_profiles(is_active, updated_at DESC);
            CREATE INDEX IF NOT EXISTS idx_portfolio_signal_reports_date ON portfolio_signal_reports(signal_date DESC, profile_id);
            """
        )
        self._ensure_column("financials", "ann_date", "TEXT")
        self._ensure_column("financials", "f_ann_date", "TEXT")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_fin_ann ON financials(ann_date)")
        self.conn.commit()

    def _ensure_column(self, table_name: str, column_name: str, column_type: str):
        existing = {
            row["name"]
            for row in self.conn.execute(f"PRAGMA table_info({table_name})").fetchall()
        }
        if column_name not in existing:
            self.conn.execute(
                f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type}"
            )

    def close(self):
        self.conn.close()

    # Settings
    def save_setting(self, key: str, value: str):
        self.conn.execute(
            "INSERT OR REPLACE INTO settings (key, value, updated_at) VALUES (?, ?, datetime('now'))",
            (key, value),
        )
        self.conn.commit()

    def get_setting(self, key: str) -> str | None:
        row = self.conn.execute(
            "SELECT value FROM settings WHERE key = ?",
            (key,),
        ).fetchone()
        return row["value"] if row else None

    def save_setting_json(self, key: str, value):
        self.save_setting(key, json.dumps(value, ensure_ascii=False, sort_keys=True))

    def get_setting_json(self, key: str, default=None):
        raw = self.get_setting(key)
        if raw is None:
            return default
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            return default

    # Stocks
    def upsert_stocks(self, stocks: list[dict]):
        self.conn.executemany(
            """
            INSERT OR REPLACE INTO stocks
                (code, name, industry, sector, area, market, list_date, updated_at)
            VALUES
                (:code, :name, :industry, :sector, :area, :market, :list_date, datetime('now'))
            """,
            stocks,
        )
        self.conn.commit()

    def count_stocks(self) -> int:
        row = self.conn.execute("SELECT COUNT(*) AS c FROM stocks").fetchone()
        return row["c"]

    def get_all_stocks(self) -> list[dict]:
        rows = self.conn.execute(
            "SELECT code, name, industry, sector, area, market, list_date FROM stocks ORDER BY code"
        ).fetchall()
        return [dict(r) for r in rows]

    def get_sectors(self) -> list[str]:
        rows = self.conn.execute(
            "SELECT DISTINCT sector FROM stocks WHERE sector IS NOT NULL AND sector != '' ORDER BY sector"
        ).fetchall()
        return [r["sector"] for r in rows]

    # Daily prices
    def upsert_prices(self, prices: list[dict]):
        if not prices:
            return
        self.conn.executemany(
            """
            INSERT OR REPLACE INTO daily_prices
                (code, trade_date, open, high, low, close, volume, amount, pct_chg, turnover)
            VALUES
                (:code, :trade_date, :open, :high, :low, :close, :volume, :amount, :pct_chg, :turnover)
            """,
            prices,
        )
        self.conn.commit()

    def get_prices(self, code: str, limit: int | None = 750) -> list[dict]:
        if limit is None:
            rows = self.conn.execute(
                "SELECT * FROM daily_prices WHERE code = ? ORDER BY trade_date",
                (code,),
            ).fetchall()
            return [dict(r) for r in rows]

        rows = self.conn.execute(
            "SELECT * FROM daily_prices WHERE code = ? ORDER BY trade_date DESC LIMIT ?",
            (code, limit),
        ).fetchall()
        return [dict(r) for r in reversed(rows)]

    def get_all_prices(self) -> dict:
        rows = self.conn.execute(
            "SELECT * FROM daily_prices ORDER BY code, trade_date"
        ).fetchall()
        result = {}
        for row in rows:
            item = dict(row)
            result.setdefault(item["code"], []).append(item)
        return result

    def get_latest_price_date(self) -> str | None:
        row = self.conn.execute(
            "SELECT MAX(trade_date) AS d FROM daily_prices"
        ).fetchone()
        return row["d"] if row else None

    # Financials
    def upsert_financials(self, records: list[dict]):
        if not records:
            return
        normalized = []
        for record in records:
            normalized.append(
                {
                    "code": record["code"],
                    "end_date": record["end_date"],
                    "ann_date": record.get("ann_date"),
                    "f_ann_date": record.get("f_ann_date"),
                    "revenue": record.get("revenue"),
                    "net_income": record.get("net_income"),
                    "gross_profit": record.get("gross_profit"),
                    "total_assets": record.get("total_assets"),
                    "total_liabilities": record.get("total_liabilities"),
                    "shareholders_equity": record.get("shareholders_equity"),
                    "operating_cash_flow": record.get("operating_cash_flow"),
                    "free_cash_flow": record.get("free_cash_flow"),
                    "eps": record.get("eps"),
                    "bps": record.get("bps"),
                    "roe": record.get("roe"),
                    "extra_json": record.get("extra_json"),
                }
            )
        self.conn.executemany(
            """
            INSERT OR REPLACE INTO financials
                (code, end_date, ann_date, f_ann_date, revenue, net_income, gross_profit, total_assets,
                 total_liabilities, shareholders_equity, operating_cash_flow,
                 free_cash_flow, eps, bps, roe, extra_json)
            VALUES
                (:code, :end_date, :ann_date, :f_ann_date, :revenue, :net_income, :gross_profit, :total_assets,
                 :total_liabilities, :shareholders_equity, :operating_cash_flow,
                 :free_cash_flow, :eps, :bps, :roe, :extra_json)
            """,
            normalized,
        )
        self.conn.commit()

    def get_financials(self, code: str, limit: int = 12) -> list[dict]:
        rows = self.conn.execute(
            "SELECT * FROM financials WHERE code = ? ORDER BY end_date DESC LIMIT ?",
            (code, limit),
        ).fetchall()
        return [dict(r) for r in reversed(rows)]

    def get_all_financials(self) -> dict:
        rows = self.conn.execute(
            "SELECT * FROM financials ORDER BY code, end_date"
        ).fetchall()
        result = {}
        for row in rows:
            item = dict(row)
            result.setdefault(item["code"], []).append(item)
        return result

    # Latest indicators
    def upsert_indicators(self, records: list[dict]):
        if not records:
            return
        self.conn.executemany(
            """
            INSERT OR REPLACE INTO indicators
                (code, pe, pe_ttm, pb, ps, ps_ttm, dv_ratio, dv_ttm, total_mv, circ_mv, updated_at)
            VALUES
                (:code, :pe, :pe_ttm, :pb, :ps, :ps_ttm, :dv_ratio, :dv_ttm, :total_mv, :circ_mv, datetime('now'))
            """,
            records,
        )
        self.conn.commit()

    def get_indicators(self, code: str) -> dict | None:
        row = self.conn.execute(
            "SELECT * FROM indicators WHERE code = ?",
            (code,),
        ).fetchone()
        return dict(row) if row else None

    # Historical indicators
    def upsert_indicator_history(self, records: list[dict]):
        if not records:
            return
        self.conn.executemany(
            """
            INSERT OR REPLACE INTO indicator_history
                (code, trade_date, pe, pe_ttm, pb, ps, ps_ttm, dv_ratio, dv_ttm, total_mv, circ_mv)
            VALUES
                (:code, :trade_date, :pe, :pe_ttm, :pb, :ps, :ps_ttm, :dv_ratio, :dv_ttm, :total_mv, :circ_mv)
            """,
            records,
        )
        self.conn.commit()

    def count_indicator_history(self) -> int:
        row = self.conn.execute(
            "SELECT COUNT(*) AS c FROM indicator_history"
        ).fetchone()
        return row["c"]

    def get_latest_indicator_history_date(self) -> str | None:
        row = self.conn.execute(
            "SELECT MAX(trade_date) AS d FROM indicator_history"
        ).fetchone()
        return row["d"] if row else None

    def get_indicator_snapshot_map(self, trade_date: str) -> dict:
        rows = self.conn.execute(
            """
            SELECT ih.*
            FROM indicator_history ih
            JOIN (
                SELECT code, MAX(trade_date) AS trade_date
                FROM indicator_history
                WHERE trade_date <= ?
                GROUP BY code
            ) latest
              ON latest.code = ih.code
             AND latest.trade_date = ih.trade_date
            """,
            (trade_date,),
        ).fetchall()
        return {row["code"]: dict(row) for row in rows}

    # Benchmark
    def upsert_benchmark(self, prices: list[dict]):
        if not prices:
            return
        self.conn.executemany(
            """
            INSERT OR REPLACE INTO benchmark_prices
                (code, trade_date, open, high, low, close, volume, amount, pct_chg)
            VALUES
                (:code, :trade_date, :open, :high, :low, :close, :volume, :amount, :pct_chg)
            """,
            prices,
        )
        self.conn.commit()

    def get_benchmark_prices(self, code: str = "000300.SH", limit: int | None = 750) -> list[dict]:
        if limit is None:
            rows = self.conn.execute(
                "SELECT * FROM benchmark_prices WHERE code = ? ORDER BY trade_date",
                (code,),
            ).fetchall()
            return [dict(r) for r in rows]

        rows = self.conn.execute(
            "SELECT * FROM benchmark_prices WHERE code = ? ORDER BY trade_date DESC LIMIT ?",
            (code, limit),
        ).fetchall()
        return [dict(r) for r in reversed(rows)]

    # Combined screener data
    def get_all_stocks_with_data(self) -> list[dict]:
        stocks = self.get_all_stocks()
        latest_market_date = self.get_latest_price_date()
        result = []
        for stock in stocks:
            code = stock["code"]
            indicators = self.get_indicators(code)
            financials = self.get_financials(code, limit=8)
            prices = self.get_prices(code, limit=250)

            if not prices:
                continue

            result.append(
                {
                    **stock,
                    "indicators": indicators or {},
                    "financials": financials,
                    "prices": prices,
                    "latest_market_date": latest_market_date,
                }
            )
        return result

    def get_stock_full(self, code: str) -> dict | None:
        row = self.conn.execute(
            "SELECT * FROM stocks WHERE code = ?",
            (code,),
        ).fetchone()
        if not row:
            return None

        stock = dict(row)
        stock["indicators"] = self.get_indicators(code) or {}
        stock["financials"] = self.get_financials(code, limit=12)
        stock["prices"] = self.get_prices(code, limit=None)
        stock["latest_market_date"] = self.get_latest_price_date()
        return stock

    # Learning snapshots
    def upsert_feature_snapshots(self, records: list[dict]):
        if not records:
            return
        self.conn.executemany(
            """
            INSERT OR REPLACE INTO feature_snapshots
                (snapshot_date, code, name, industry, sector, rank, composite_score,
                 value_score, growth_score, quality_score, momentum_score,
                 price, pe, pb, roe, revenue_growth, net_income_growth,
                 momentum_20d, momentum_60d, weights_json, filters_json,
                 sectors_json, extra_json)
            VALUES
                (:snapshot_date, :code, :name, :industry, :sector, :rank, :composite_score,
                 :value_score, :growth_score, :quality_score, :momentum_score,
                 :price, :pe, :pb, :roe, :revenue_growth, :net_income_growth,
                 :momentum_20d, :momentum_60d, :weights_json, :filters_json,
                 :sectors_json, :extra_json)
            """,
            records,
        )
        self.conn.commit()

    def delete_feature_snapshots(self, snapshot_date: str):
        self.conn.execute(
            "DELETE FROM feature_snapshots WHERE snapshot_date = ?",
            (snapshot_date,),
        )
        self.conn.commit()

    def count_feature_snapshots(self) -> int:
        row = self.conn.execute(
            "SELECT COUNT(*) AS c FROM feature_snapshots"
        ).fetchone()
        return row["c"]

    def get_latest_snapshot_date(self) -> str | None:
        row = self.conn.execute(
            "SELECT MAX(snapshot_date) AS d FROM feature_snapshots"
        ).fetchone()
        return row["d"] if row else None

    def get_snapshot_dates(self, limit: int = 30) -> list[str]:
        rows = self.conn.execute(
            "SELECT DISTINCT snapshot_date FROM feature_snapshots ORDER BY snapshot_date DESC LIMIT ?",
            (limit,),
        ).fetchall()
        return [r["snapshot_date"] for r in rows]

    def get_feature_snapshots(self, snapshot_date: str, limit: int | None = None) -> list[dict]:
        if limit is None:
            rows = self.conn.execute(
                "SELECT * FROM feature_snapshots WHERE snapshot_date = ? ORDER BY rank ASC",
                (snapshot_date,),
            ).fetchall()
        else:
            rows = self.conn.execute(
                "SELECT * FROM feature_snapshots WHERE snapshot_date = ? ORDER BY rank ASC LIMIT ?",
                (snapshot_date, limit),
            ).fetchall()
        return [dict(r) for r in rows]

    def get_unlabeled_snapshots(self, horizon_days: int, limit: int = 100000) -> list[dict]:
        rows = self.conn.execute(
            """
            SELECT fs.*
            FROM feature_snapshots fs
            LEFT JOIN learning_labels ll
              ON ll.snapshot_date = fs.snapshot_date
             AND ll.code = fs.code
             AND ll.horizon_days = ?
            WHERE ll.code IS NULL
            ORDER BY fs.snapshot_date ASC, fs.rank ASC
            LIMIT ?
            """,
            (horizon_days, limit),
        ).fetchall()
        return [dict(r) for r in rows]

    # Learning labels
    def upsert_learning_labels(self, records: list[dict]):
        if not records:
            return
        self.conn.executemany(
            """
            INSERT OR REPLACE INTO learning_labels
                (snapshot_date, code, horizon_days, entry_date, exit_date,
                 entry_price, exit_price, stock_return, benchmark_return, alpha)
            VALUES
                (:snapshot_date, :code, :horizon_days, :entry_date, :exit_date,
                 :entry_price, :exit_price, :stock_return, :benchmark_return, :alpha)
            """,
            records,
        )
        self.conn.commit()

    def count_learning_labels(self) -> int:
        row = self.conn.execute(
            "SELECT COUNT(*) AS c FROM learning_labels"
        ).fetchone()
        return row["c"]

    def get_learning_label_stats(self) -> list[dict]:
        rows = self.conn.execute(
            """
            SELECT horizon_days, COUNT(*) AS label_count, MAX(snapshot_date) AS latest_snapshot_date
            FROM learning_labels
            GROUP BY horizon_days
            ORDER BY horizon_days
            """
        ).fetchall()
        return [dict(r) for r in rows]

    def get_labeled_rows(self, horizon_days: int) -> list[dict]:
        rows = self.conn.execute(
            """
            SELECT fs.snapshot_date, fs.code, fs.rank, fs.composite_score,
                   fs.value_score, fs.growth_score, fs.quality_score, fs.momentum_score,
                   ll.stock_return, ll.benchmark_return, ll.alpha
            FROM feature_snapshots fs
            JOIN learning_labels ll
              ON ll.snapshot_date = fs.snapshot_date
             AND ll.code = fs.code
            WHERE ll.horizon_days = ?
            ORDER BY fs.snapshot_date DESC, fs.rank ASC
            """,
            (horizon_days,),
        ).fetchall()
        return [dict(r) for r in rows]

    def get_model_training_rows(
        self,
        horizon_days: int,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> list[dict]:
        sql = """
            SELECT fs.snapshot_date, fs.code, fs.name, fs.industry, fs.sector, fs.rank,
                   fs.composite_score, fs.value_score, fs.growth_score,
                   fs.quality_score, fs.momentum_score, fs.price, fs.pe, fs.pb,
                   fs.roe, fs.revenue_growth, fs.net_income_growth,
                   fs.momentum_20d, fs.momentum_60d, fs.extra_json,
                   ll.stock_return, ll.benchmark_return, ll.alpha
            FROM feature_snapshots fs
            JOIN learning_labels ll
              ON ll.snapshot_date = fs.snapshot_date
             AND ll.code = fs.code
            WHERE ll.horizon_days = ?
        """
        params = [horizon_days]
        if start_date:
            sql += " AND fs.snapshot_date >= ?"
            params.append(start_date)
        if end_date:
            sql += " AND fs.snapshot_date <= ?"
            params.append(end_date)
        sql += " ORDER BY fs.snapshot_date ASC, fs.rank ASC"
        rows = self.conn.execute(sql, tuple(params)).fetchall()
        return [dict(r) for r in rows]

    # Learning runs and reports
    def upsert_learning_run(self, record: dict):
        self.conn.execute(
            """
            INSERT OR REPLACE INTO learning_runs
                (run_date, top_n, total_candidates, snapshot_count, active_weights_json,
                 learned_weights_json, label_stats_json, meta_json, updated_at)
            VALUES
                (:run_date, :top_n, :total_candidates, :snapshot_count, :active_weights_json,
                 :learned_weights_json, :label_stats_json, :meta_json, datetime('now'))
            """,
            record,
        )
        self.conn.commit()

    def get_latest_learning_run(self) -> dict | None:
        row = self.conn.execute(
            "SELECT * FROM learning_runs ORDER BY run_date DESC LIMIT 1"
        ).fetchone()
        return dict(row) if row else None

    def upsert_review_report(self, report_date: str, learning_horizon: int, summary_md: str, stats: dict):
        self.conn.execute(
            """
            INSERT OR REPLACE INTO review_reports
                (report_date, learning_horizon, summary_md, stats_json, updated_at)
            VALUES
                (?, ?, ?, ?, datetime('now'))
            """,
            (
                report_date,
                learning_horizon,
                summary_md,
                json.dumps(stats, ensure_ascii=False, sort_keys=True),
            ),
        )
        self.conn.commit()

    def get_latest_review_report(self) -> dict | None:
        row = self.conn.execute(
            "SELECT * FROM review_reports ORDER BY report_date DESC LIMIT 1"
        ).fetchone()
        return dict(row) if row else None

    # Models
    def activate_model(self, model_id: str):
        self.conn.execute("UPDATE model_registry SET is_active = 0")
        self.conn.execute(
            "UPDATE model_registry SET is_active = 1, updated_at = datetime('now') WHERE model_id = ?",
            (model_id,),
        )
        self.conn.commit()
        self.save_setting("active_model_id", model_id)

    def upsert_model_registry(self, record: dict, activate: bool = False):
        self.conn.execute(
            """
            INSERT OR REPLACE INTO model_registry
                (model_id, model_type, horizon_days, train_start_date, train_end_date,
                 validation_start_date, validation_end_date, feature_names_json,
                 metrics_json, artifact_path, train_rows, validation_rows,
                 is_active, extra_json, updated_at)
            VALUES
                (:model_id, :model_type, :horizon_days, :train_start_date, :train_end_date,
                 :validation_start_date, :validation_end_date, :feature_names_json,
                 :metrics_json, :artifact_path, :train_rows, :validation_rows,
                 :is_active, :extra_json, datetime('now'))
            """,
            record,
        )
        self.conn.commit()
        if activate:
            self.activate_model(record["model_id"])

    def get_latest_model_registry(self, horizon_days: int | None = None, active_only: bool = False) -> dict | None:
        sql = "SELECT * FROM model_registry"
        params = []
        clauses = []
        if horizon_days is not None:
            clauses.append("horizon_days = ?")
            params.append(horizon_days)
        if active_only:
            clauses.append("is_active = 1")
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)
        sql += " ORDER BY created_at DESC LIMIT 1"
        row = self.conn.execute(sql, tuple(params)).fetchone()
        return dict(row) if row else None

    def get_model_registry(self, model_id: str) -> dict | None:
        row = self.conn.execute(
            "SELECT * FROM model_registry WHERE model_id = ?",
            (model_id,),
        ).fetchone()
        return dict(row) if row else None

    def upsert_model_predictions(self, records: list[dict]):
        if not records:
            return
        self.conn.executemany(
            """
            INSERT OR REPLACE INTO model_predictions
                (model_id, snapshot_date, code, name, sector, baseline_rank,
                 composite_score, model_score, model_rank)
            VALUES
                (:model_id, :snapshot_date, :code, :name, :sector, :baseline_rank,
                 :composite_score, :model_score, :model_rank)
            """,
            records,
        )
        self.conn.commit()

    def get_model_predictions(
        self,
        model_id: str,
        snapshot_date: str | None = None,
        limit: int | None = 20,
    ) -> list[dict]:
        sql = "SELECT * FROM model_predictions WHERE model_id = ?"
        params = [model_id]
        if snapshot_date is not None:
            sql += " AND snapshot_date = ?"
            params.append(snapshot_date)
        sql += " ORDER BY snapshot_date DESC, model_rank ASC"
        if limit is not None:
            sql += " LIMIT ?"
            params.append(limit)
        rows = self.conn.execute(sql, tuple(params)).fetchall()
        return [dict(r) for r in rows]

    def count_model_registry(self) -> int:
        row = self.conn.execute(
            "SELECT COUNT(*) AS c FROM model_registry"
        ).fetchone()
        return row["c"]

    def count_model_predictions(self) -> int:
        row = self.conn.execute(
            "SELECT COUNT(*) AS c FROM model_predictions"
        ).fetchone()
        return row["c"]

    def upsert_model_monitor_report(self, report_date: str, model_id: str, horizon_days: int, summary_md: str, stats: dict):
        self.conn.execute(
            """
            INSERT OR REPLACE INTO model_monitor_reports
                (report_date, model_id, horizon_days, summary_md, stats_json, updated_at)
            VALUES
                (?, ?, ?, ?, ?, datetime('now'))
            """,
            (
                report_date,
                model_id,
                horizon_days,
                summary_md,
                json.dumps(stats, ensure_ascii=False, sort_keys=True),
            ),
        )
        self.conn.commit()

    def get_latest_model_monitor_report(self, horizon_days: int | None = None) -> dict | None:
        sql = "SELECT * FROM model_monitor_reports"
        params = []
        if horizon_days is not None:
            sql += " WHERE horizon_days = ?"
            params.append(horizon_days)
        sql += " ORDER BY report_date DESC, updated_at DESC LIMIT 1"
        row = self.conn.execute(sql, tuple(params)).fetchone()
        return dict(row) if row else None

    def get_model_monitor_reports(self, limit: int = 30, horizon_days: int | None = None) -> list[dict]:
        sql = "SELECT * FROM model_monitor_reports"
        params = []
        if horizon_days is not None:
            sql += " WHERE horizon_days = ?"
            params.append(horizon_days)
        sql += " ORDER BY report_date DESC, updated_at DESC"
        if limit is not None:
            sql += " LIMIT ?"
            params.append(limit)
        rows = self.conn.execute(sql, tuple(params)).fetchall()
        return [dict(r) for r in rows]


    def count_model_monitor_reports(self) -> int:
        row = self.conn.execute(
            "SELECT COUNT(*) AS c FROM model_monitor_reports"
        ).fetchone()
        return row["c"]

    def upsert_portfolio_profile(
        self,
        profile_id: str,
        name: str,
        optimize_for: str,
        config: dict,
        metrics: dict,
        extra: dict | None = None,
        is_active: bool = False,
    ):
        self.conn.execute(
            """
            INSERT OR REPLACE INTO portfolio_profiles
                (profile_id, name, optimize_for, config_json, metrics_json, extra_json, is_active, updated_at)
            VALUES
                (?, ?, ?, ?, ?, ?, ?, datetime('now'))
            """,
            (
                profile_id,
                name,
                optimize_for,
                json.dumps(config, ensure_ascii=False, sort_keys=True),
                json.dumps(metrics, ensure_ascii=False, sort_keys=True),
                json.dumps(extra or {}, ensure_ascii=False, sort_keys=True),
                1 if is_active else 0,
            ),
        )
        self.conn.commit()

    def activate_portfolio_profile(self, profile_id: str):
        self.conn.execute("UPDATE portfolio_profiles SET is_active = 0")
        self.conn.execute(
            "UPDATE portfolio_profiles SET is_active = 1, updated_at = datetime('now') WHERE profile_id = ?",
            (profile_id,),
        )
        self.conn.commit()

    def get_portfolio_profile(self, profile_id: str) -> dict | None:
        row = self.conn.execute(
            "SELECT * FROM portfolio_profiles WHERE profile_id = ?",
            (profile_id,),
        ).fetchone()
        return dict(row) if row else None

    def get_active_portfolio_profile(self) -> dict | None:
        row = self.conn.execute(
            "SELECT * FROM portfolio_profiles WHERE is_active = 1 ORDER BY updated_at DESC LIMIT 1"
        ).fetchone()
        return dict(row) if row else None

    def get_portfolio_profiles(self, limit: int = 20, active_only: bool = False) -> list[dict]:
        sql = "SELECT * FROM portfolio_profiles"
        params = []
        if active_only:
            sql += " WHERE is_active = 1"
        sql += " ORDER BY is_active DESC, updated_at DESC"
        if limit is not None:
            sql += " LIMIT ?"
            params.append(limit)
        rows = self.conn.execute(sql, tuple(params)).fetchall()
        return [dict(r) for r in rows]

    def count_portfolio_profiles(self) -> int:
        row = self.conn.execute("SELECT COUNT(*) AS c FROM portfolio_profiles").fetchone()
        return row["c"]

    def upsert_portfolio_signal_report(
        self,
        signal_date: str,
        profile_id: str,
        model_id: str | None,
        summary_md: str,
        stats: dict,
    ):
        self.conn.execute(
            """
            INSERT OR REPLACE INTO portfolio_signal_reports
                (signal_date, profile_id, model_id, summary_md, stats_json, updated_at)
            VALUES
                (?, ?, ?, ?, ?, datetime('now'))
            """,
            (
                signal_date,
                profile_id,
                model_id,
                summary_md,
                json.dumps(stats, ensure_ascii=False, sort_keys=True),
            ),
        )
        self.conn.commit()

    def get_latest_portfolio_signal_report(self, profile_id: str | None = None) -> dict | None:
        sql = "SELECT * FROM portfolio_signal_reports"
        params = []
        if profile_id is not None:
            sql += " WHERE profile_id = ?"
            params.append(profile_id)
        sql += " ORDER BY signal_date DESC, updated_at DESC LIMIT 1"
        row = self.conn.execute(sql, tuple(params)).fetchone()
        return dict(row) if row else None

    def get_portfolio_signal_reports(self, limit: int = 30, profile_id: str | None = None) -> list[dict]:
        sql = "SELECT * FROM portfolio_signal_reports"
        params = []
        if profile_id is not None:
            sql += " WHERE profile_id = ?"
            params.append(profile_id)
        sql += " ORDER BY signal_date DESC, updated_at DESC"
        if limit is not None:
            sql += " LIMIT ?"
            params.append(limit)
        rows = self.conn.execute(sql, tuple(params)).fetchall()
        return [dict(r) for r in rows]

    def count_portfolio_signal_reports(self) -> int:
        row = self.conn.execute("SELECT COUNT(*) AS c FROM portfolio_signal_reports").fetchone()
        return row["c"]

    # Utility
    def has_table_data(self, table_name: str) -> bool:
        try:
            row = self.conn.execute(f"SELECT COUNT(*) AS c FROM {table_name}").fetchone()
            return row["c"] > 0
        except Exception:
            return False

    def cache_age_hours(self) -> float | None:
        last = self.get_setting("last_refresh")
        if not last:
            return None
        try:
            dt = datetime.fromisoformat(last)
            delta = datetime.now() - dt
            return round(delta.total_seconds() / 3600, 1)
        except ValueError:
            return None
