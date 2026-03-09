"""
Point-in-time data builder for backtests and historical learning.
"""
from bisect import bisect_right
from datetime import datetime, timedelta


class PointInTimeDataBuilder:
    def __init__(self, db, stocks=None, all_prices=None, financials_by_code=None):
        self.db = db
        self.stocks = stocks or db.get_all_stocks()
        self.all_prices = all_prices or db.get_all_prices()
        self.financials_by_code = financials_by_code or db.get_all_financials()
        self.price_dates = {
            code: [row["trade_date"] for row in rows]
            for code, rows in self.all_prices.items()
        }

    def build_universe(
        self,
        snapshot_date: str,
        include_indicators: bool = True,
        min_price_history: int = 20,
        price_limit: int = 250,
    ) -> list[dict]:
        indicator_map = {}
        if include_indicators and self.db.count_indicator_history() > 0:
            indicator_map = self.db.get_indicator_snapshot_map(snapshot_date)

        financial_map = self._financial_map_as_of(snapshot_date)
        result = []
        for stock in self.stocks:
            code = stock["code"]
            prices = self._slice_prices_at_date(
                code=code,
                snapshot_date=snapshot_date,
                min_price_history=min_price_history,
                price_limit=price_limit,
            )
            if not prices:
                continue

            result.append(
                {
                    **stock,
                    "prices": prices,
                    "financials": financial_map.get(code, []),
                    "indicators": indicator_map.get(code, {}),
                    "latest_market_date": snapshot_date,
                }
            )
        return result

    def _slice_prices_at_date(
        self,
        code: str,
        snapshot_date: str,
        min_price_history: int,
        price_limit: int,
    ) -> list[dict]:
        rows = self.all_prices.get(code)
        dates = self.price_dates.get(code)
        if not rows or not dates:
            return []

        idx = bisect_right(dates, snapshot_date) - 1
        if idx < 0 or dates[idx] != snapshot_date:
            return []

        if idx + 1 < min_price_history:
            return []

        start = max(0, idx - price_limit + 1)
        sliced = rows[start : idx + 1]
        if len(sliced) < min_price_history:
            return []
        return sliced

    def _financial_map_as_of(self, snapshot_date: str, limit_per_code: int = 8) -> dict:
        result = {}
        for code, rows in self.financials_by_code.items():
            available = []
            for row in rows:
                available_date = self.financial_available_date(row)
                if available_date and available_date <= snapshot_date:
                    available.append(row)
            if available:
                result[code] = available[-limit_per_code:]
        return result

    @classmethod
    def financial_available_date(cls, row: dict) -> str | None:
        for key in ("f_ann_date", "ann_date"):
            value = row.get(key)
            if cls._is_trade_date(value):
                return value

        end_date = row.get("end_date")
        if not cls._is_trade_date(end_date):
            return None

        dt = datetime.strptime(end_date, "%Y%m%d")
        lag_days = {
            "0331": 30,
            "0630": 62,
            "0930": 31,
            "1231": 120,
        }.get(end_date[4:], 45)
        return (dt + timedelta(days=lag_days)).strftime("%Y%m%d")

    @staticmethod
    def _is_trade_date(value) -> bool:
        return isinstance(value, str) and len(value) == 8 and value.isdigit()
