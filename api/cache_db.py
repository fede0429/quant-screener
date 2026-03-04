"""
SQLite 缓存层 — 存储 Tushare 拉取的数据，避免重复请求
"""
import sqlite3
import json
import time
import logging
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


class CacheDB:
    def __init__(self, db_path: str):
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        self._create_tables()
        logger.info(f"SQLite 缓存数据库已打开: {db_path}")

    def _create_tables(self):
        self.conn.executescript("""
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

        CREATE INDEX IF NOT EXISTS idx_prices_code ON daily_prices(code);
        CREATE INDEX IF NOT EXISTS idx_prices_date ON daily_prices(trade_date);
        CREATE INDEX IF NOT EXISTS idx_fin_code ON financials(code);
        CREATE INDEX IF NOT EXISTS idx_bench_date ON benchmark_prices(trade_date);
        """)
        self.conn.commit()

    def close(self):
        self.conn.close()

    # ─── Settings ──────────────────────────────────────────────────────────
    def save_setting(self, key: str, value: str):
        self.conn.execute(
            "INSERT OR REPLACE INTO settings (key, value, updated_at) VALUES (?, ?, datetime('now'))",
            (key, value)
        )
        self.conn.commit()

    def get_setting(self, key: str) -> str | None:
        row = self.conn.execute("SELECT value FROM settings WHERE key = ?", (key,)).fetchone()
        return row["value"] if row else None

    # ─── Stocks ────────────────────────────────────────────────────────────
    def upsert_stocks(self, stocks: list[dict]):
        self.conn.executemany(
            """INSERT OR REPLACE INTO stocks (code, name, industry, sector, area, market, list_date, updated_at)
               VALUES (:code, :name, :industry, :sector, :area, :market, :list_date, datetime('now'))""",
            stocks
        )
        self.conn.commit()

    def count_stocks(self) -> int:
        row = self.conn.execute("SELECT COUNT(*) as c FROM stocks").fetchone()
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

    # ─── Daily Prices ──────────────────────────────────────────────────────
    def upsert_prices(self, prices: list[dict]):
        if not prices:
            return
        self.conn.executemany(
            """INSERT OR REPLACE INTO daily_prices
               (code, trade_date, open, high, low, close, volume, amount, pct_chg, turnover)
               VALUES (:code, :trade_date, :open, :high, :low, :close, :volume, :amount, :pct_chg, :turnover)""",
            prices
        )
        self.conn.commit()

    def get_prices(self, code: str, limit: int = 750) -> list[dict]:
        rows = self.conn.execute(
            "SELECT * FROM daily_prices WHERE code = ? ORDER BY trade_date DESC LIMIT ?",
            (code, limit)
        ).fetchall()
        return [dict(r) for r in reversed(rows)]

    def get_all_prices(self) -> dict:
        """Return {code: [price_rows]} for all stocks"""
        rows = self.conn.execute(
            "SELECT * FROM daily_prices ORDER BY code, trade_date"
        ).fetchall()
        result = {}
        for r in rows:
            d = dict(r)
            code = d["code"]
            if code not in result:
                result[code] = []
            result[code].append(d)
        return result

    def get_latest_price_date(self) -> str | None:
        row = self.conn.execute("SELECT MAX(trade_date) as d FROM daily_prices").fetchone()
        return row["d"] if row else None

    # ─── Financials ────────────────────────────────────────────────────────
    def upsert_financials(self, records: list[dict]):
        if not records:
            return
        self.conn.executemany(
            """INSERT OR REPLACE INTO financials
               (code, end_date, revenue, net_income, gross_profit, total_assets,
                total_liabilities, shareholders_equity, operating_cash_flow,
                free_cash_flow, eps, bps, roe, extra_json)
               VALUES (:code, :end_date, :revenue, :net_income, :gross_profit, :total_assets,
                :total_liabilities, :shareholders_equity, :operating_cash_flow,
                :free_cash_flow, :eps, :bps, :roe, :extra_json)""",
            records
        )
        self.conn.commit()

    def get_financials(self, code: str, limit: int = 12) -> list[dict]:
        rows = self.conn.execute(
            "SELECT * FROM financials WHERE code = ? ORDER BY end_date DESC LIMIT ?",
            (code, limit)
        ).fetchall()
        return [dict(r) for r in reversed(rows)]

    # ─── Indicators (PE/PB etc) ────────────────────────────────────────────
    def upsert_indicators(self, records: list[dict]):
        if not records:
            return
        self.conn.executemany(
            """INSERT OR REPLACE INTO indicators
               (code, pe, pe_ttm, pb, ps, ps_ttm, dv_ratio, dv_ttm, total_mv, circ_mv, updated_at)
               VALUES (:code, :pe, :pe_ttm, :pb, :ps, :ps_ttm, :dv_ratio, :dv_ttm, :total_mv, :circ_mv, datetime('now'))""",
            records
        )
        self.conn.commit()

    def get_indicators(self, code: str) -> dict | None:
        row = self.conn.execute("SELECT * FROM indicators WHERE code = ?", (code,)).fetchone()
        return dict(row) if row else None

    # ─── Benchmark ─────────────────────────────────────────────────────────
    def upsert_benchmark(self, prices: list[dict]):
        if not prices:
            return
        self.conn.executemany(
            """INSERT OR REPLACE INTO benchmark_prices
               (code, trade_date, open, high, low, close, volume, amount, pct_chg)
               VALUES (:code, :trade_date, :open, :high, :low, :close, :volume, :amount, :pct_chg)""",
            prices
        )
        self.conn.commit()

    def get_benchmark_prices(self, code: str = "000300.SH", limit: int = 750) -> list[dict]:
        rows = self.conn.execute(
            "SELECT * FROM benchmark_prices WHERE code = ? ORDER BY trade_date DESC LIMIT ?",
            (code, limit)
        ).fetchall()
        return [dict(r) for r in reversed(rows)]

    # ─── Combined Query for Screener ───────────────────────────────────────
    def get_all_stocks_with_data(self) -> list[dict]:
        """获取所有股票 + 最新指标 + 最近财务 + 最近行情（供因子计算）"""
        stocks = self.get_all_stocks()
        result = []
        for s in stocks:
            code = s["code"]
            indicators = self.get_indicators(code)
            financials = self.get_financials(code, limit=8)
            prices = self.get_prices(code, limit=250)

            if not prices:
                continue

            result.append({
                **s,
                "indicators": indicators or {},
                "financials": financials,
                "prices": prices
            })
        return result

    def get_stock_full(self, code: str) -> dict | None:
        """获取个股完整信息"""
        rows = self.conn.execute("SELECT * FROM stocks WHERE code = ?", (code,)).fetchone()
        if not rows:
            return None

        stock = dict(rows)
        stock["indicators"] = self.get_indicators(code) or {}
        stock["financials"] = self.get_financials(code, limit=12)
        stock["prices"] = self.get_prices(code, limit=750)
        return stock

    # ─── Utility ──────────────────────────────────────────────────────────
    def has_table_data(self, table_name: str) -> bool:
        try:
            row = self.conn.execute(f"SELECT COUNT(*) as c FROM {table_name}").fetchone()
            return row["c"] > 0
        except:
            return False

    def cache_age_hours(self) -> float | None:
        last = self.get_setting("last_refresh")
        if not last:
            return None
        try:
            dt = datetime.fromisoformat(last)
            delta = datetime.now() - dt
            return round(delta.total_seconds() / 3600, 1)
        except:
            return None
