"""
Multi-factor scoring engine with default risk controls.
"""
import logging
import math
from typing import Optional

logger = logging.getLogger(__name__)

DEFAULT_RISK_POLICY = {
    "exclude_st": True,
    "exclude_suspended": True,
    "min_avg_amount_20d": 20000.0,
    "min_avg_turnover_20d": 0.0,
    "min_price": 2.0,
    "min_market_cap": 20.0,
}


class FactorEngine:
    def score_and_rank(
        self,
        stocks: list[dict],
        weights: dict,
        filters: dict = None,
        sectors: list = None,
    ) -> list[dict]:
        if not stocks:
            return []

        filters = filters or {}
        sectors = sectors or []
        risk_policy = self.get_risk_policy(filters)

        factor_data = []
        for stock in stocks:
            factors = self._compute_raw_factors(stock)
            if not factors:
                continue

            factor_data.append(
                {
                    "code": stock["code"],
                    "name": stock["name"],
                    "industry": stock.get("industry", ""),
                    "sector": stock.get("sector", ""),
                    "factors": factors,
                    "risk": self._compute_risk_profile(stock, factors, risk_policy),
                    "stock": stock,
                }
            )

        if not factor_data:
            return []

        factor_data = self._apply_filters(factor_data, filters, risk_policy)
        if sectors:
            factor_data = [item for item in factor_data if item["sector"] in sectors]

        if not factor_data:
            return []

        self._normalize_factors(factor_data)

        total_weight = sum(weights.values()) or 1
        norm_weights = {key: value / total_weight for key, value in weights.items()}
        for item in factor_data:
            scores = item.get("scores", {})
            composite = (
                scores.get("value_score", 50) * norm_weights.get("value", 0.25)
                + scores.get("growth_score", 50) * norm_weights.get("growth", 0.25)
                + scores.get("quality_score", 50) * norm_weights.get("quality", 0.25)
                + scores.get("momentum_score", 50) * norm_weights.get("momentum", 0.25)
            )
            item["composite_score"] = round(composite, 1)

        factor_data.sort(key=lambda item: item["composite_score"], reverse=True)

        results = []
        for rank, item in enumerate(factor_data, 1):
            factors = item["factors"]
            scores = item.get("scores", {})
            risk = item["risk"]
            prices = item["stock"].get("prices", [])
            current_price = prices[-1]["close"] if prices else None

            results.append(
                {
                    "rank": rank,
                    "code": item["code"],
                    "name": item["name"],
                    "industry": item["industry"],
                    "sector": item["sector"],
                    "composite_score": item["composite_score"],
                    "value_score": round(scores.get("value_score", 0), 1),
                    "growth_score": round(scores.get("growth_score", 0), 1),
                    "quality_score": round(scores.get("quality_score", 0), 1),
                    "momentum_score": round(scores.get("momentum_score", 0), 1),
                    "price": current_price,
                    "pe": factors.get("pe"),
                    "pb": factors.get("pb"),
                    "roe": factors.get("roe"),
                    "revenue_growth": factors.get("revenue_growth_yoy"),
                    "net_income_growth": factors.get("net_income_growth_yoy"),
                    "momentum_20d": factors.get("momentum_20d"),
                    "momentum_60d": factors.get("momentum_60d"),
                    "momentum_120d": factors.get("momentum_120d"),
                    "dividend_yield": factors.get("dividend_yield"),
                    "market_cap": factors.get("market_cap"),
                    "gross_margin": factors.get("gross_margin"),
                    "debt_ratio": factors.get("debt_ratio"),
                    "fcf_yield": factors.get("fcf_yield"),
                    "rsi_14": factors.get("rsi_14"),
                    "volume_ratio": factors.get("volume_ratio"),
                    "avg_amount_20d": factors.get("avg_amount_20d"),
                    "avg_turnover_20d": factors.get("avg_turnover_20d"),
                    "volatility_20d": factors.get("volatility_20d"),
                    "volatility_60d": factors.get("volatility_60d"),
                    "max_drawdown_60d": factors.get("max_drawdown_60d"),
                    "price_vs_ma20": factors.get("price_vs_ma20"),
                    "price_vs_ma60": factors.get("price_vs_ma60"),
                    "is_st": risk.get("is_st"),
                    "is_suspended": risk.get("is_suspended"),
                    "latest_trade_date": risk.get("latest_trade_date"),
                    "latest_market_date": risk.get("latest_market_date"),
                    "risk_flags": risk.get("risk_flags", []),
                }
            )

        return results

    def get_risk_policy(self, filters: dict | None = None) -> dict:
        filters = filters or {}
        policy = dict(DEFAULT_RISK_POLICY)

        if "exclude_st" in filters:
            policy["exclude_st"] = bool(filters["exclude_st"])
        if filters.get("allow_st"):
            policy["exclude_st"] = False

        if "exclude_suspended" in filters:
            policy["exclude_suspended"] = bool(filters["exclude_suspended"])
        if filters.get("allow_suspended"):
            policy["exclude_suspended"] = False

        for key in ("min_avg_amount_20d", "min_avg_turnover_20d", "min_price", "min_market_cap"):
            if key in filters and filters[key] is not None:
                policy[key] = float(filters[key])

        return policy

    def _compute_raw_factors(self, stock: dict) -> Optional[dict]:
        indicators = stock.get("indicators", {})
        financials = stock.get("financials", [])
        prices = stock.get("prices", [])

        if not prices or len(prices) < 20:
            return None

        closes = [row["close"] for row in prices if row.get("close")]
        if len(closes) < 20:
            return None

        pe = indicators.get("pe_ttm") or indicators.get("pe")
        pb = indicators.get("pb")
        ps = indicators.get("ps_ttm") or indicators.get("ps")
        dividend_yield = indicators.get("dv_ttm") or indicators.get("dv_ratio", 0)
        market_cap = indicators.get("total_mv")
        if market_cap:
            market_cap = market_cap / 10000

        revenue_growth = None
        net_income_growth = None
        roe = None
        if financials and len(financials) >= 2:
            sorted_financials = sorted(financials, key=lambda row: row["end_date"], reverse=True)
            latest = sorted_financials[0]
            latest_date = latest["end_date"]
            previous_year = f"{int(latest_date[:4]) - 1}{latest_date[4:]}"
            previous = next(
                (row for row in sorted_financials if row["end_date"] == previous_year),
                None,
            )

            if previous and latest.get("revenue") and previous.get("revenue") and previous["revenue"] != 0:
                revenue_growth = round((latest["revenue"] / previous["revenue"] - 1) * 100, 2)

            if previous and latest.get("net_income") and previous.get("net_income") and previous["net_income"] != 0:
                net_income_growth = round((latest["net_income"] / previous["net_income"] - 1) * 100, 2)

            roe = latest.get("roe")
            if not roe and latest.get("net_income") and latest.get("shareholders_equity"):
                equity = latest["shareholders_equity"]
                if equity:
                    roe = round(latest["net_income"] / equity * 100, 2)

        gross_margin = None
        debt_ratio = None
        fcf_yield = None
        if financials:
            latest_financial = max(financials, key=lambda row: row["end_date"])
            if latest_financial.get("revenue") and latest_financial.get("gross_profit") and latest_financial["revenue"] != 0:
                gross_margin = round(latest_financial["gross_profit"] / latest_financial["revenue"] * 100, 2)

            if (
                latest_financial.get("total_liabilities")
                and latest_financial.get("total_assets")
                and latest_financial["total_assets"] != 0
            ):
                debt_ratio = round(
                    latest_financial["total_liabilities"] / latest_financial["total_assets"] * 100,
                    2,
                )

            if latest_financial.get("free_cash_flow") and market_cap and market_cap > 0:
                fcf_yield = round(latest_financial["free_cash_flow"] / (market_cap * 1e8) * 100, 2)

        def momentum(series: list[float], period: int) -> float | None:
            if len(series) < period + 1:
                return None
            return round((series[-1] / series[-period - 1] - 1) * 100, 2)

        def moving_average(series: list[float], period: int) -> float | None:
            if len(series) < period:
                return None
            return sum(series[-period:]) / period

        def rolling_std_pct(series: list[float], period: int) -> float | None:
            if len(series) < period + 1:
                return None
            returns = []
            for idx in range(len(series) - period, len(series)):
                prev_close = series[idx - 1]
                close = series[idx]
                if prev_close:
                    returns.append((close / prev_close - 1) * 100)
            if len(returns) < 2:
                return None
            mean = sum(returns) / len(returns)
            variance = sum((value - mean) ** 2 for value in returns) / len(returns)
            return round(variance ** 0.5, 4)

        def max_drawdown_pct(series: list[float], period: int) -> float | None:
            if len(series) < period:
                return None
            trailing = series[-period:]
            peak = trailing[0]
            worst = 0.0
            for value in trailing:
                peak = max(peak, value)
                if peak:
                    drawdown = (value / peak - 1) * 100
                    worst = min(worst, drawdown)
            return round(worst, 2)

        def relative_to_ma(price: float, ma_value: float | None) -> float | None:
            if not price or not ma_value:
                return None
            return round((price / ma_value - 1) * 100, 2)

        current_price = closes[-1]
        momentum_20d = momentum(closes, 20)
        momentum_60d = momentum(closes, 60)
        momentum_120d = momentum(closes, 120)
        ma20 = moving_average(closes, 20)
        ma60 = moving_average(closes, 60)

        volumes = [row.get("volume", 0) for row in prices if row.get("volume") is not None]
        amounts = [row.get("amount", 0) for row in prices if row.get("amount") is not None]
        turnovers = [row.get("turnover", 0) for row in prices if row.get("turnover") is not None]

        avg_volume_5 = moving_average(volumes, 5) if volumes else None
        avg_volume_20 = moving_average(volumes, 20) if volumes else None
        volume_ratio = (
            round(avg_volume_5 / avg_volume_20, 2)
            if avg_volume_5 and avg_volume_20 and avg_volume_20 != 0
            else None
        )

        rsi_14 = self._rsi(closes, 14)

        return {
            "pe": pe,
            "pb": pb,
            "ps": ps,
            "dividend_yield": dividend_yield,
            "market_cap": market_cap,
            "revenue_growth_yoy": revenue_growth,
            "net_income_growth_yoy": net_income_growth,
            "roe": roe,
            "gross_margin": gross_margin,
            "debt_ratio": debt_ratio,
            "fcf_yield": fcf_yield,
            "momentum_20d": momentum_20d,
            "momentum_60d": momentum_60d,
            "momentum_120d": momentum_120d,
            "rsi_14": rsi_14,
            "volume_ratio": volume_ratio,
            "avg_amount_20d": self._rolling_average(amounts, 20),
            "avg_turnover_20d": self._rolling_average(turnovers, 20),
            "volatility_20d": rolling_std_pct(closes, 20),
            "volatility_60d": rolling_std_pct(closes, 60),
            "max_drawdown_60d": max_drawdown_pct(closes, 60),
            "price_vs_ma20": relative_to_ma(current_price, ma20),
            "price_vs_ma60": relative_to_ma(current_price, ma60),
            "above_ma20": current_price > ma20 if ma20 else None,
            "above_ma60": current_price > ma60 if ma60 else None,
        }

    def _compute_risk_profile(self, stock: dict, factors: dict, risk_policy: dict) -> dict:
        prices = stock.get("prices", [])
        latest_trade_date = prices[-1]["trade_date"] if prices else None
        latest_market_date = stock.get("latest_market_date") or latest_trade_date
        current_price = prices[-1]["close"] if prices else None
        market_cap = factors.get("market_cap")
        avg_amount_20d = factors.get("avg_amount_20d")
        avg_turnover_20d = factors.get("avg_turnover_20d")

        is_st = self._is_special_treatment(stock.get("name"))
        is_suspended = bool(latest_trade_date and latest_market_date and latest_trade_date != latest_market_date)

        risk_flags = []
        if is_st:
            risk_flags.append("st")
        if is_suspended:
            risk_flags.append("suspended")
        if avg_amount_20d is not None and avg_amount_20d < risk_policy["min_avg_amount_20d"]:
            risk_flags.append("low_amount")
        if avg_turnover_20d is not None and avg_turnover_20d < risk_policy["min_avg_turnover_20d"]:
            risk_flags.append("low_turnover")
        if current_price is not None and current_price < risk_policy["min_price"]:
            risk_flags.append("low_price")
        if market_cap is not None and market_cap < risk_policy["min_market_cap"]:
            risk_flags.append("micro_cap")

        return {
            "is_st": is_st,
            "is_suspended": is_suspended,
            "latest_trade_date": latest_trade_date,
            "latest_market_date": latest_market_date,
            "risk_flags": risk_flags,
        }

    def _apply_filters(self, data: list, filters: dict, risk_policy: dict) -> list:
        result = []
        for item in data:
            factors = item["factors"]
            risk = item["risk"]
            current_price = item["stock"].get("prices", [{}])[-1].get("close")
            passed = True

            if risk_policy["exclude_st"] and risk.get("is_st"):
                passed = False
            if risk_policy["exclude_suspended"] and risk.get("is_suspended"):
                passed = False

            if (
                risk_policy.get("min_avg_amount_20d") is not None
                and factors.get("avg_amount_20d") is not None
                and factors["avg_amount_20d"] < risk_policy["min_avg_amount_20d"]
            ):
                passed = False
            if (
                risk_policy.get("min_avg_turnover_20d") is not None
                and factors.get("avg_turnover_20d") is not None
                and factors["avg_turnover_20d"] < risk_policy["min_avg_turnover_20d"]
            ):
                passed = False
            if (
                risk_policy.get("min_price") is not None
                and current_price is not None
                and current_price < risk_policy["min_price"]
            ):
                passed = False
            if (
                risk_policy.get("min_market_cap") is not None
                and factors.get("market_cap") is not None
                and factors["market_cap"] < risk_policy["min_market_cap"]
            ):
                passed = False

            if "pe_min" in filters and factors.get("pe") is not None and factors["pe"] < filters["pe_min"]:
                passed = False
            if "pe_max" in filters and factors.get("pe") is not None and factors["pe"] > filters["pe_max"]:
                passed = False

            if "pb_min" in filters and factors.get("pb") is not None and factors["pb"] < filters["pb_min"]:
                passed = False
            if "pb_max" in filters and factors.get("pb") is not None and factors["pb"] > filters["pb_max"]:
                passed = False

            if (
                "revenue_growth_min" in filters
                and factors.get("revenue_growth_yoy") is not None
                and factors["revenue_growth_yoy"] < filters["revenue_growth_min"]
            ):
                passed = False

            if "roe_min" in filters and factors.get("roe") is not None and factors["roe"] < filters["roe_min"]:
                passed = False

            if (
                "gross_margin_min" in filters
                and factors.get("gross_margin") is not None
                and factors["gross_margin"] < filters["gross_margin_min"]
            ):
                passed = False

            if (
                "debt_ratio_max" in filters
                and factors.get("debt_ratio") is not None
                and factors["debt_ratio"] > filters["debt_ratio_max"]
            ):
                passed = False

            if (
                "momentum_20d_min" in filters
                and factors.get("momentum_20d") is not None
                and factors["momentum_20d"] < filters["momentum_20d_min"]
            ):
                passed = False
            if (
                "momentum_20d_max" in filters
                and factors.get("momentum_20d") is not None
                and factors["momentum_20d"] > filters["momentum_20d_max"]
            ):
                passed = False

            if "rsi_min" in filters and factors.get("rsi_14") is not None and factors["rsi_14"] < filters["rsi_min"]:
                passed = False
            if "rsi_max" in filters and factors.get("rsi_14") is not None and factors["rsi_14"] > filters["rsi_max"]:
                passed = False

            if passed:
                result.append(item)

        return result

    def _normalize_factors(self, data: list):
        factor_groups = {
            "value": {
                "pe": {"direction": -1},
                "pb": {"direction": -1},
                "dividend_yield": {"direction": 1},
                "ps": {"direction": -1},
            },
            "growth": {
                "revenue_growth_yoy": {"direction": 1},
                "net_income_growth_yoy": {"direction": 1},
                "roe": {"direction": 1},
            },
            "quality": {
                "gross_margin": {"direction": 1},
                "debt_ratio": {"direction": -1},
                "fcf_yield": {"direction": 1},
            },
            "momentum": {
                "momentum_20d": {"direction": 1},
                "momentum_60d": {"direction": 1},
                "rsi_14": {"direction": 0},
                "volume_ratio": {"direction": 1},
            },
        }

        for factors in factor_groups.values():
            for factor_name, config in factors.items():
                values = []
                for item in data:
                    value = item["factors"].get(factor_name)
                    if self._is_valid_number(value):
                        values.append(value)

                if len(values) < 2:
                    for item in data:
                        item.setdefault("z_scores", {})[factor_name] = 0
                    continue

                mean = sum(values) / len(values)
                variance = sum((value - mean) ** 2 for value in values) / len(values)
                std = variance ** 0.5 or 1

                for item in data:
                    value = item["factors"].get(factor_name)
                    if self._is_valid_number(value):
                        z_score = (value - mean) / std
                        if config["direction"] == -1:
                            z_score = -z_score
                        elif config["direction"] == 0:
                            z_score = -abs(value - 57.5) / 15
                        z_score = max(-3, min(3, z_score))
                    else:
                        z_score = 0
                    item.setdefault("z_scores", {})[factor_name] = z_score

        for item in data:
            z_scores = item.get("z_scores", {})

            def group_score(factor_names: list[str]) -> float:
                values = [z_scores.get(name, 0) for name in factor_names]
                average = sum(values) / len(values) if values else 0
                percentile = 50 + average * 16.67
                return max(0, min(100, percentile))

            item.setdefault("scores", {})
            item["scores"]["value_score"] = group_score(["pe", "pb", "dividend_yield", "ps"])
            item["scores"]["growth_score"] = group_score(
                ["revenue_growth_yoy", "net_income_growth_yoy", "roe"]
            )
            item["scores"]["quality_score"] = group_score(["gross_margin", "debt_ratio", "fcf_yield"])
            item["scores"]["momentum_score"] = group_score(
                ["momentum_20d", "momentum_60d", "rsi_14", "volume_ratio"]
            )

    @staticmethod
    def _rolling_average(values: list[float], period: int) -> float | None:
        usable = [value for value in values if value is not None]
        if len(usable) < period:
            return None
        return round(sum(usable[-period:]) / period, 4)

    @staticmethod
    def _rsi(closes: list[float], period: int = 14) -> float:
        if len(closes) < period + 1:
            return 50.0
        gains = []
        losses = []
        for index in range(-period, 0):
            diff = closes[index] - closes[index - 1]
            gains.append(max(0, diff))
            losses.append(max(0, -diff))
        avg_gain = sum(gains) / period
        avg_loss = sum(losses) / period
        if avg_loss == 0:
            return 100.0
        rs = avg_gain / avg_loss
        return round(100 - 100 / (1 + rs), 2)

    @staticmethod
    def _is_special_treatment(name: str | None) -> bool:
        if not name:
            return False
        upper_name = str(name).upper()
        return "ST" in upper_name or "退" in upper_name

    @staticmethod
    def _is_valid_number(value) -> bool:
        return value is not None and not math.isnan(value) and not math.isinf(value)
