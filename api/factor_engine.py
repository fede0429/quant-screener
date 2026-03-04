"""
因子计算引擎 — 多因子评分与排名
价值因子 / 成长因子 / 质量因子 / 动量因子
"""
import math
import logging
from typing import Optional

logger = logging.getLogger(__name__)


class FactorEngine:

    # ─── 主入口 ────────────────────────────────────────────────────────────
    def score_and_rank(
        self,
        stocks: list[dict],
        weights: dict,
        filters: dict = None,
        sectors: list = None
    ) -> list[dict]:
        """
        计算因子 → 标准化 → 加权打分 → 排名
        Returns: 排序后的结果列表
        """
        if not stocks:
            return []

        # 1. 计算每只股票的原始因子値
        factor_data = []
        for stock in stocks:
            factors = self._compute_raw_factors(stock)
            if factors:
                factor_data.append({
                    "code": stock["code"],
                    "name": stock["name"],
                    "industry": stock.get("industry", ""),
                    "sector": stock.get("sector", ""),
                    "factors": factors,
                    "stock": stock  # 保留原始数据
                })

        if not factor_data:
            return []

        # 2. 应用过滤器
        if filters:
            factor_data = self._apply_filters(factor_data, filters)

        # 3. 板块筛选
        if sectors:
            factor_data = [f for f in factor_data if f["sector"] in sectors]

        if not factor_data:
            return []

        # 4. Z-Score 标准化 + 百分位转换
        self._normalize_factors(factor_data)

        # 5. 加权合成得分
        total_weight = sum(weights.values()) or 1
        norm_weights = {k: v / total_weight for k, v in weights.items()}

        for item in factor_data:
            scores = item.get("scores", {})
            composite = (
                scores.get("value_score", 50) * norm_weights.get("value", 0.25) +
                scores.get("growth_score", 50) * norm_weights.get("growth", 0.25) +
                scores.get("quality_score", 50) * norm_weights.get("quality", 0.25) +
                scores.get("momentum_score", 50) * norm_weights.get("momentum", 0.25)
            )
            item["composite_score"] = round(composite, 1)

        # 6. 排名
        factor_data.sort(key=lambda x: x["composite_score"], reverse=True)

        # 7. 组装返回结果
        results = []
        for rank, item in enumerate(factor_data, 1):
            f = item["factors"]
            s = item.get("scores", {})
            stock = item["stock"]
            prices = stock.get("prices", [])
            current_price = prices[-1]["close"] if prices else None

            results.append({
                "rank": rank,
                "code": item["code"],
                "name": item["name"],
                "industry": item["industry"],
                "sector": item["sector"],
                "composite_score": item["composite_score"],
                "value_score": round(s.get("value_score", 0), 1),
                "growth_score": round(s.get("growth_score", 0), 1),
                "quality_score": round(s.get("quality_score", 0), 1),
                "momentum_score": round(s.get("momentum_score", 0), 1),
                "price": current_price,
                "pe": f.get("pe"),
                "pb": f.get("pb"),
                "roe": f.get("roe"),
                "revenue_growth": f.get("revenue_growth_yoy"),
                "net_income_growth": f.get("net_income_growth_yoy"),
                "momentum_20d": f.get("momentum_20d"),
                "momentum_60d": f.get("momentum_60d"),
                "dividend_yield": f.get("dividend_yield"),
                "market_cap": f.get("market_cap"),
                "gross_margin": f.get("gross_margin"),
                "debt_ratio": f.get("debt_ratio"),
            })

        return results

    # ─── 原始因子计算 ───────────────────────────────────────────────────
    def _compute_raw_factors(self, stock: dict) -> Optional[dict]:
        """从原始数据计算各维度因子"""
        indicators = stock.get("indicators", {})
        financials = stock.get("financials", [])
        prices = stock.get("prices", [])

        if not prices or len(prices) < 20:
            return None

        closes = [p["close"] for p in prices if p.get("close")]
        if len(closes) < 20:
            return None

        # ─── 价值因子 ──────────────────────────────────────────────────────
        pe = indicators.get("pe_ttm") or indicators.get("pe")
        pb = indicators.get("pb")
        ps = indicators.get("ps_ttm") or indicators.get("ps")
        dv = indicators.get("dv_ttm") or indicators.get("dv_ratio", 0)
        market_cap = indicators.get("total_mv")  # 万元
        if market_cap:
            market_cap = market_cap / 10000  # 转为亿元

        # ─── 成长因子 ──────────────────────────────────────────────────────
        revenue_growth = None
        ni_growth = None
        roe = None

        if financials and len(financials) >= 2:
            # 取最新两期年报做同比
            sorted_fin = sorted(financials, key=lambda x: x["end_date"], reverse=True)
            latest = sorted_fin[0]
            # 找上一年同期
            latest_date = latest["end_date"]
            prev_year_date = str(int(latest_date[:4]) - 1) + latest_date[4:]
            prev = next((f for f in sorted_fin if f["end_date"] == prev_year_date), None)

            if prev and latest.get("revenue") and prev.get("revenue") and prev["revenue"] != 0:
                revenue_growth = round((latest["revenue"] / prev["revenue"] - 1) * 100, 2)

            if prev and latest.get("net_income") and prev.get("net_income") and prev["net_income"] != 0:
                ni_growth = round((latest["net_income"] / prev["net_income"] - 1) * 100, 2)

            roe = latest.get("roe")
            if not roe and latest.get("net_income") and latest.get("shareholders_equity"):
                se = latest["shareholders_equity"]
                if se and se != 0:
                    roe = round(latest["net_income"] / se * 100, 2)

        # ─── 质量因子 ──────────────────────────────────────────────────────
        gross_margin = None
        debt_ratio = None
        current_ratio = None  # 暂不计算
        fcf_yield = None

        if financials:
            latest_fin = max(financials, key=lambda x: x["end_date"])
            if latest_fin.get("revenue") and latest_fin.get("gross_profit") and latest_fin["revenue"] != 0:
                gross_margin = round(latest_fin["gross_profit"] / latest_fin["revenue"] * 100, 2)

            if latest_fin.get("total_liabilities") and latest_fin.get("total_assets") and latest_fin["total_assets"] != 0:
                debt_ratio = round(latest_fin["total_liabilities"] / latest_fin["total_assets"] * 100, 2)

            if latest_fin.get("free_cash_flow") and market_cap and market_cap > 0:
                fcf_yield = round(latest_fin["free_cash_flow"] / (market_cap * 1e8) * 100, 2)

        # ─── 动量因子 ──────────────────────────────────────────────────────
        def momentum(data, period):
            if len(data) < period + 1:
                return None
            return round((data[-1] / data[-period - 1] - 1) * 100, 2)

        def rsi(data, period=14):
            if len(data) < period + 1:
                return 50
            gains, losses = [], []
            for i in range(-period, 0):
                diff = data[i] - data[i - 1]
                gains.append(max(0, diff))
                losses.append(max(0, -diff))
            avg_gain = sum(gains) / period
            avg_loss = sum(losses) / period
            if avg_loss == 0:
                return 100
            rs = avg_gain / avg_loss
            return round(100 - 100 / (1 + rs), 2)

        def ma(data, period):
            if len(data) < period:
                return None
            return sum(data[-period:]) / period

        mom_20d = momentum(closes, 20)
        mom_60d = momentum(closes, 60)
        mom_120d = momentum(closes, 120)
        rsi_14 = rsi(closes, 14)

        volumes = [p.get("volume", 0) for p in prices if p.get("volume")]
        vol_5 = ma(volumes, 5) if volumes else None
        vol_20 = ma(volumes, 20) if volumes else None
        vol_ratio = round(vol_5 / vol_20, 2) if vol_5 and vol_20 and vol_20 != 0 else None

        ma5 = ma(closes, 5)
        ma20 = ma(closes, 20)
        ma60 = ma(closes, 60)

        return {
            # 价值
            "pe": pe,
            "pb": pb,
            "ps": ps,
            "dividend_yield": dv,
            "market_cap": market_cap,
            # 成长
            "revenue_growth_yoy": revenue_growth,
            "net_income_growth_yoy": ni_growth,
            "roe": roe,
            # 质量
            "gross_margin": gross_margin,
            "debt_ratio": debt_ratio,
            "fcf_yield": fcf_yield,
            # 动量
            "momentum_20d": mom_20d,
            "momentum_60d": mom_60d,
            "momentum_120d": mom_120d,
            "rsi_14": rsi_14,
            "volume_ratio": vol_ratio,
            "above_ma20": closes[-1] > ma20 if ma20 else None,
            "above_ma60": closes[-1] > ma60 if ma60 else None,
        }

    # ─── 过滤器 ────────────────────────────────────────────────────────────
    def _apply_filters(self, data: list, filters: dict) -> list:
        """应用用户设定的阈値过滤"""
        result = []
        for item in data:
            f = item["factors"]
            passed = True

            # PE 范围
            if "pe_min" in filters and f.get("pe") is not None:
                if f["pe"] < filters["pe_min"]:
                    passed = False
            if "pe_max" in filters and f.get("pe") is not None:
                if f["pe"] > filters["pe_max"]:
                    passed = False

            # PB 范围
            if "pb_min" in filters and f.get("pb") is not None:
                if f["pb"] < filters["pb_min"]:
                    passed = False
            if "pb_max" in filters and f.get("pb") is not None:
                if f["pb"] > filters["pb_max"]:
                    passed = False

            # 营收增长
            if "revenue_growth_min" in filters and f.get("revenue_growth_yoy") is not None:
                if f["revenue_growth_yoy"] < filters["revenue_growth_min"]:
                    passed = False

            # ROE
            if "roe_min" in filters and f.get("roe") is not None:
                if f["roe"] < filters["roe_min"]:
                    passed = False

            # 毛利率
            if "gross_margin_min" in filters and f.get("gross_margin") is not None:
                if f["gross_margin"] < filters["gross_margin_min"]:
                    passed = False

            # 资产负债率上限
            if "debt_ratio_max" in filters and f.get("debt_ratio") is not None:
                if f["debt_ratio"] > filters["debt_ratio_max"]:
                    passed = False

            # 动量
            if "momentum_20d_min" in filters and f.get("momentum_20d") is not None:
                if f["momentum_20d"] < filters["momentum_20d_min"]:
                    passed = False
            if "momentum_20d_max" in filters and f.get("momentum_20d") is not None:
                if f["momentum_20d"] > filters["momentum_20d_max"]:
                    passed = False

            # RSI
            if "rsi_min" in filters and f.get("rsi_14") is not None:
                if f["rsi_14"] < filters["rsi_min"]:
                    passed = False
            if "rsi_max" in filters and f.get("rsi_14") is not None:
                if f["rsi_14"] > filters["rsi_max"]:
                    passed = False

            if passed:
                result.append(item)

        return result

    # ─── Z-Score 标准化 + 百分位 ──────────────────────────────────────────
    def _normalize_factors(self, data: list):
        """
        对每个因子维度做 Z-Score 标准化，再转为 0-100 百分位得分。
        注意方向：PE/PB/debt_ratio 越低越好 → 取反
        """
        factor_groups = {
            "value": {
                "pe": {"direction": -1, "default": 50},      # 越低越好
                "pb": {"direction": -1, "default": 50},
                "dividend_yield": {"direction": 1, "default": 0},
                "ps": {"direction": -1, "default": 50},
            },
            "growth": {
                "revenue_growth_yoy": {"direction": 1, "default": 0},
                "net_income_growth_yoy": {"direction": 1, "default": 0},
                "roe": {"direction": 1, "default": 0},
            },
            "quality": {
                "gross_margin": {"direction": 1, "default": 0},
                "debt_ratio": {"direction": -1, "default": 50},
                "fcf_yield": {"direction": 1, "default": 0},
            },
            "momentum": {
                "momentum_20d": {"direction": 1, "default": 0},
                "momentum_60d": {"direction": 1, "default": 0},
                "rsi_14": {"direction": 0, "default": 50},  # 最优区间 40-70
                "volume_ratio": {"direction": 1, "default": 1},
            }
        }

        # 计算每个因子的均値和标准差
        for group_name, factors in factor_groups.items():
            for factor_name, config in factors.items():
                values = []
                for item in data:
                    v = item["factors"].get(factor_name)
                    if v is not None and not math.isnan(v) and not math.isinf(v):
                        values.append(v)

                if not values or len(values) < 2:
                    for item in data:
                        item.setdefault("z_scores", {})[factor_name] = 0
                    continue

                mean = sum(values) / len(values)
                std = (sum((v - mean) ** 2 for v in values) / len(values)) ** 0.5
                if std == 0:
                    std = 1

                for item in data:
                    v = item["factors"].get(factor_name)
                    if v is not None and not math.isnan(v) and not math.isinf(v):
                        z = (v - mean) / std
                        if config["direction"] == -1:
                            z = -z
                        elif config["direction"] == 0:
                            # RSI: penalize extremes, optimal around 50-65
                            z = -abs(v - 57.5) / 15  # normalized distance from optimal
                        # Clip to [-3, 3]
                        z = max(-3, min(3, z))
                        item.setdefault("z_scores", {})[factor_name] = z
                    else:
                        item.setdefault("z_scores", {})[factor_name] = 0

        # 计算各维度得分 (Z-Score → 0-100 百分位)
        for item in data:
            zs = item.get("z_scores", {})

            def group_score(factor_names):
                vals = [zs.get(fn, 0) for fn in factor_names]
                avg_z = sum(vals) / len(vals) if vals else 0
                # Z-Score to percentile (roughly: z=0 → 50, z=3 → ~99)
                pct = 50 + avg_z * 16.67  # maps [-3,3] to [0,100]
                return max(0, min(100, pct))

            item.setdefault("scores", {})
            item["scores"]["value_score"] = group_score(
                ["pe", "pb", "dividend_yield", "ps"]
            )
            item["scores"]["growth_score"] = group_score(
                ["revenue_growth_yoy", "net_income_growth_yoy", "roe"]
            )
            item["scores"]["quality_score"] = group_score(
                ["gross_margin", "debt_ratio", "fcf_yield"]
            )
            item["scores"]["momentum_score"] = group_score(
                ["momentum_20d", "momentum_60d", "rsi_14", "volume_ratio"]
            )
