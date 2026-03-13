"""
SQLite 缓存数据库模块

修复说明：
- 强化连接初始化
- 增加 transaction 上下文
- upsert 支持 commit=False
- 增加批量读取辅助接口，降低 N+1 风险
"""
import json
import logging
import sqlite3
from contextlib import contextmanager
from pathlib import Path

logger = logging.getLogger(__name__)


class CacheDB:
    def __init__(self, db_path: str = "data/cache.db"):
        self.db_path = db_path
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(
            db_path,
            check_same_thread=False,
            timeout=30.0,
        )
        self.conn.row_factory = sqlite3.Row
        self._configure_connection()
        self.init_tables()

    def _configure_connection(self):
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA busy_timeout = 30000")
        self.conn.execute("PRAGMA synchronous = NORMAL")
        self.conn.execute("PRAGMA foreign_keys = ON")

    @contextmanager
    def transaction(self):
        try:
            yield self.conn
            self.conn.commit()
        except Exception:
            self.conn.rollback()
            raise

    def init_tables(self):
        cur = self.conn.cursor()
        cur.execute("""
        CREATE TABLE IF NOT EXISTS stocks (
            code TEXT PRIMARY KEY,
            name TEXT,
            industry TEXT,
            sector TEXT,
            area TEXT,
            market TEXT,
            list_date TEXT,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
        """)
        cur.execute("""
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
        )
        """)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS financials (
            code TEXT,
            end_date TEXT,
            ann_date TEXT,
            f_ann_date TEXT,
            revenue REAL,
            n_income REAL,
            total_assets REAL,
            total_hldr_eqy_exc_min_int REAL,
            n_cashflow_act REAL,
            roe REAL,
            gross_margin REAL,
            debt_ratio REAL,
            PRIMARY KEY (code, end_date, ann_date)
        )
        """)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS indicators (
            code TEXT PRIMARY KEY,
            trade_date TEXT,
            pe REAL,
            pb REAL,
            market_cap REAL,
            dividend_yield REAL,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
        """)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS indicator_history (
            code TEXT,
            trade_date TEXT,
            pe REAL,
            pb REAL,
            market_cap REAL,
            dividend_yield REAL,
            PRIMARY KEY (code, trade_date)
        )
        """)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS benchmark_prices (
            code TEXT,
            trade_date TEXT,
            close REAL,
            PRIMARY KEY (code, trade_date)
        )
        """)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS settings (
            key TEXT PRIMARY KEY,
            value TEXT
        )
        """)
        self.conn.commit()

    def save_setting(self, key: str, value: str):
        self.conn.execute(
            "INSERT OR REPLACE INTO settings(key, value) VALUES (?, ?)",
            (key, value),
        )
        self.conn.commit()

    def get_setting(self, key: str, default=None):
        row = self.conn.execute("SELECT value FROM settings WHERE key = ?", (key,)).fetchone()
        return row["value"] if row else default

    def upsert_stocks(self, rows: list[dict], commit: bool = True):
        self.conn.executemany("""
            INSERT OR REPLACE INTO stocks
            (code, name, industry, sector, area, market, list_date, updated_at)
            VALUES (:code, :name, :industry, :sector, :area, :market, :list_date, CURRENT_TIMESTAMP)
        """, rows)
        if commit:
            self.conn.commit()

    def upsert_prices(self, rows: list[dict], commit: bool = True):
        self.conn.executemany("""
            INSERT OR REPLACE INTO daily_prices
            (code, trade_date, open, high, low, close, volume, amount, pct_chg, turnover)
            VALUES (:code, :trade_date, :open, :high, :low, :close, :volume, :amount, :pct_chg, :turnover)
        """, rows)
        if commit:
            self.conn.commit()

    def upsert_financials(self, rows: list[dict], commit: bool = True):
        self.conn.executemany("""
            INSERT OR REPLACE INTO financials
            (code, end_date, ann_date, f_ann_date, revenue, n_income, total_assets,
             total_hldr_eqy_exc_min_int, n_cashflow_act, roe, gross_margin, debt_ratio)
            VALUES (:code, :end_date, :ann_date, :f_ann_date, :revenue, :n_income, :total_assets,
                    :total_hldr_eqy_exc_min_int, :n_cashflow_act, :roe, :gross_margin, :debt_ratio)
        """, rows)
        if commit:
            self.conn.commit()

    def upsert_indicators(self, rows: list[dict], commit: bool = True):
        self.conn.executemany("""
            INSERT OR REPLACE INTO indicators
            (code, trade_date, pe, pb, market_cap, dividend_yield, updated_at)
            VALUES (:code, :trade_date, :pe, :pb, :market_cap, :dividend_yield, CURRENT_TIMESTAMP)
        """, rows)
        if commit:
            self.conn.commit()

    def upsert_indicator_history(self, rows: list[dict], commit: bool = True):
        self.conn.executemany("""
            INSERT OR REPLACE INTO indicator_history
            (code, trade_date, pe, pb, market_cap, dividend_yield)
            VALUES (:code, :trade_date, :pe, :pb, :market_cap, :dividend_yield)
        """, rows)
        if commit:
            self.conn.commit()

    def upsert_benchmark_prices(self, rows: list[dict], commit: bool = True):
        self.conn.executemany("""
            INSERT OR REPLACE INTO benchmark_prices
            (code, trade_date, close)
            VALUES (:code, :trade_date, :close)
        """, rows)
        if commit:
            self.conn.commit()

    def get_all_stocks(self) -> list[dict]:
        rows = self.conn.execute("SELECT * FROM stocks ORDER BY code").fetchall()
        return [dict(r) for r in rows]

    def get_all_prices(self) -> dict:
        rows = self.conn.execute("""
            SELECT * FROM daily_prices
            ORDER BY code, trade_date
        """).fetchall()
        grouped = {}
        for row in rows:
            item = dict(row)
            grouped.setdefault(item["code"], []).append(item)
        return grouped

    def get_all_financials(self) -> dict:
        rows = self.conn.execute("""
            SELECT * FROM financials
            ORDER BY code, end_date, ann_date
        """).fetchall()
        grouped = {}
        for row in rows:
            item = dict(row)
            grouped.setdefault(item["code"], []).append(item)
        return grouped

    def get_all_indicators(self) -> list[dict]:
        rows = self.conn.execute("SELECT * FROM indicators ORDER BY code").fetchall()
        return [dict(r) for r in rows]

    def get_prices_for_codes(self, codes: list[str]) -> dict:
        if not codes:
            return {}
        placeholders = ",".join(["?"] * len(codes))
        rows = self.conn.execute(
            f"SELECT * FROM daily_prices WHERE code IN ({placeholders}) ORDER BY code, trade_date",
            codes,
        ).fetchall()
        grouped = {}
        for row in rows:
            item = dict(row)
            grouped.setdefault(item["code"], []).append(item)
        return grouped

    def get_financials_for_codes(self, codes: list[str]) -> dict:
        if not codes:
            return {}
        placeholders = ",".join(["?"] * len(codes))
        rows = self.conn.execute(
            f"SELECT * FROM financials WHERE code IN ({placeholders}) ORDER BY code, end_date, ann_date",
            codes,
        ).fetchall()
        grouped = {}
        for row in rows:
            item = dict(row)
            grouped.setdefault(item["code"], []).append(item)
        return grouped

    def get_indicators_for_codes(self, codes: list[str]) -> dict:
        if not codes:
            return {}
        placeholders = ",".join(["?"] * len(codes))
        rows = self.conn.execute(
            f"SELECT * FROM indicators WHERE code IN ({placeholders}) ORDER BY code",
            codes,
        ).fetchall()
        grouped = {}
        for row in rows:
            item = dict(row)
            grouped[item["code"]] = item
        return grouped

    def get_latest_price_date(self):
        row = self.conn.execute("SELECT MAX(trade_date) AS max_date FROM daily_prices").fetchone()
        return row["max_date"] if row else None

    def count_indicator_history(self) -> int:
        row = self.conn.execute("SELECT COUNT(*) AS cnt FROM indicator_history").fetchone()
        return int(row["cnt"]) if row else 0

    def get_indicator_snapshot_map(self, trade_date: str) -> dict:
        rows = self.conn.execute("""
            SELECT * FROM indicator_history
            WHERE trade_date = ?
        """, (trade_date,)).fetchall()
        return {dict(r)["code"]: dict(r) for r in rows}

    def close(self):
        if self.conn:
            self.conn.close()
            self.conn = None
