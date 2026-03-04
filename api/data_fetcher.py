"""
Tushare Pro 数据获取模块
负责从 Tushare API 拉取 A 股数据并存入 SQLite 缓存
"""
import logging
import time
from datetime import datetime, timedelta
from cache_db import CacheDB

logger = logging.getLogger(__name__)

# Tushare 限频：每分钟约 200 次（普通用户）
RATE_LIMIT_DELAY = 0.35  # 秒


class TushareDataFetcher:
    def __init__(self, db: CacheDB):
        self.db = db
        self.pro = None
        self.token = None

    def set_token(self, token: str) -> dict:
        """设置 Token 并验证"""
        try:
            import tushare as ts
            ts.set_token(token)
            pro = ts.pro_api(token)

            # 验证：拉取交易日历（最轻量的接口）
            df = pro.trade_cal(exchange='SSE', start_date='20260101', end_date='20260105')
            if df is None or df.empty:
                return {"success": False, "error": "Token 无效或接口不可用"}

            self.pro = pro
            self.token = token
            logger.info("✅ Tushare Token 验证成功")
            return {"success": True, "info": f"已连接 Tushare Pro，交易日历正常"}

        except ImportError:
            return {"success": False, "error": "未安装 tushare 包，请运行 pip install tushare"}
        except Exception as e:
            logger.error(f"Token 验证失败: {e}")
            return {"success": False, "error": str(e)}

    def is_connected(self) -> bool:
        return self.pro is not None

    def _ensure_connected(self):
        if not self.pro:
            raise RuntimeError("Tushare 未连接，请先设置 Token")

    # ─── 核心刷新流程 ────────────────────────────────────────────────────
    def refresh_all(self, full_refresh: bool = False) -> dict:
        """
        全量或增量刷新数据
        full_refresh=True: 股票列表 + 3年日线 + 财务报表 + 估值指标 + 沪深300
        full_refresh=False: 只更新最近行情 + 估值指标
        """
        self._ensure_connected()
        stats = {}

        # 1. 股票列表（始终刷新）
        stats["stocks"] = self._fetch_stock_list()

        # 2. 日线行情
        if full_refresh:
            stats["prices"] = self._fetch_all_prices(days=750)
        else:
            stats["prices"] = self._fetch_incremental_prices()

        # 3. 财务数据（仅全量刷新时拉取，因为耗时长）
        if full_refresh:
            stats["financials"] = self._fetch_financials()

        # 4. 估值指标（每次都刷新）
        stats["indicators"] = self._fetch_indicators()

        # 5. 沪深300基准
        stats["benchmark"] = self._fetch_benchmark(days=750 if full_refresh else 60)

        # 记录刷新时间
        self.db.save_setting("last_refresh", datetime.now().isoformat())

        return stats

    # ─── 股票列表 ─────────────────────────────────────────────────────
    def _fetch_stock_list(self) -> dict:
        """获取 A 股股票列表"""
        logger.info("📋 获取 A 股股票列表...")
        try:
            df = self.pro.stock_basic(
                exchange='',
                list_status='L',
                fields='ts_code,symbol,name,industry,area,market,list_date'
            )
            if df is None or df.empty:
                return {"count": 0, "error": "未获取到数据"}

            # 映射 Tushare 的行业到板块
            sector_map = self._build_sector_map()

            stocks = []
            for _, row in df.iterrows():
                industry = row.get("industry", "")
                stocks.append({
                    "code": row["ts_code"],
                    "name": row["name"],
                    "industry": industry,
                    "sector": sector_map.get(industry, "其他"),
                    "area": row.get("area", ""),
                    "market": row.get("market", ""),
                    "list_date": row.get("list_date", ""),
                })

            self.db.upsert_stocks(stocks)
            logger.info(f"   ✅ 获取 {len(stocks)} 只股票")
            return {"count": len(stocks)}

        except Exception as e:
            logger.error(f"获取股票列表失败: {e}")
            return {"count": 0, "error": str(e)}

    def _build_sector_map(self) -> dict:
        """行业→板块映射"""
        return {
            # 金融
            "銀行": "金融", "保险": "金融", "证券": "金融", "多元金融": "金融",
            "互联网金融": "金融",
            # 消费
            "白酒": "消费", "食品饮料": "消费", "家电": "消费", "调味品": "消费",
            "乳制品": "消费", "免税": "消费", "零售": "消费", "纶织服装": "消费",
            "家居用品": "消费", "鄯酒行业": "消费", "食品加工": "消费", "饮料制造": "消费",
            "家用电器": "消费", "商业百货": "消费", "旅游酒店": "消费",
            # 科技
            "半导体": "科技", "电子": "科技", "软件": "科技", "通信": "科技",
            "安防": "科技", "面板": "科技", "芯片": "科技", "AI语音": "科技",
            "光模块": "科技", "半导体设备": "科技", "计算机": "科技",
            "通信设备": "科技", "电子制造": "科技", "IT服务": "科技",
            "互联网": "科技", "游戏": "科技",
            # 医药
            "医药": "医药", "医疗器械": "医药", "医疗服务": "医药", "CXO": "医药",
            "中药": "医药", "化学制药": "医药", "生物制品": "医药",
            "医药商业": "医药", "医疗保健": "医药",
            # 新能源
            "电池": "新能源", "光伏": "新能源", "逆变器": "新能源", "风电": "新能源",
            "新能源": "新能源", "储能": "新能源", "锂矿": "新能源",
            "新能源汽车": "新能源",
            # 资源
            "黄金": "资源", "煞炭": "资源", "有色金属": "资源", "石油": "资源",
            "稀土": "资源", "锤锂": "资源", "采掘": "资源",
            # 制造
            "工程机械": "制造", "化工": "制造", "汽车": "制造", "工控": "制造",
            "机械设备": "制造", "汽车配件": "制造", "军工": "制造",
            "航空航天": "制造", "船舶": "制造", "电气设备": "制造",
            # 基建
            "建筑": "基建", "水泥": "基建", "建材": "基建", "房地产": "基建",
            "房地产开发": "基建", "基础建设": "基建", "装饰装修": "基建",
            # 公用事业
            "电力": "公用事业", "燃气": "公用事业", "水务": "公用事业",
            "环保": "公用事业", "公用事业": "公用事业",
            # 物流
            "物流": "物流", "航运": "物流", "港口": "物流", "机场": "物流",
            "交通运输": "物流", "高速公路": "物流",
            # 农业
            "养殖": "农业", "农业": "农业", "农产品": "农业", "种植业": "农业",
            # 传媒
            "传媒": "传媒", "广告": "传媒", "影视": "传媒", "出版": "传媒",
        }

    # ─── 日线行情 ─────────────────────────────────────────────────────
    def _fetch_all_prices(self, days: int = 750) -> dict:
        """全量拉取所有股票日线（分批）"""
        logger.info(f"📈 全量获取日线行情（最近 {days} 天）...")
        stocks = self.db.get_all_stocks()
        end_date = datetime.now().strftime("%Y%m%d")
        start_date = (datetime.now() - timedelta(days=int(days * 1.5))).strftime("%Y%m%d")

        total = 0
        errors = 0
        for i, stock in enumerate(stocks):
            code = stock["code"]
            try:
                df = self.pro.daily(
                    ts_code=code,
                    start_date=start_date,
                    end_date=end_date
                )
                if df is not None and not df.empty:
                    prices = []
                    for _, row in df.iterrows():
                        prices.append({
                            "code": code,
                            "trade_date": row["trade_date"],
                            "open": row.get("open"),
                            "high": row.get("high"),
                            "low": row.get("low"),
                            "close": row.get("close"),
                            "volume": row.get("vol"),
                            "amount": row.get("amount"),
                            "pct_chg": row.get("pct_chg"),
                            "turnover": row.get("turnover_rate", 0) if "turnover_rate" in row.index else 0,
                        })
                    self.db.upsert_prices(prices)
                    total += len(prices)

                if (i + 1) % 50 == 0:
                    logger.info(f"   进度: {i+1}/{len(stocks)} 只, 共 {total} 条")
                    self.db.conn.commit()

                time.sleep(RATE_LIMIT_DELAY)

            except Exception as e:
                errors += 1
                logger.warning(f"   ⚠ {code} 行情获取失败: {e}")
                time.sleep(1)

        logger.info(f"   ✅ 行情获取完成: {total} 条, {errors} 个错误")
        return {"records": total, "errors": errors}

    def _fetch_incremental_prices(self) -> dict:
        """增量拉取：只获取最近缺失的行情"""
        logger.info("📈 增量更新日线行情...")
        last_date = self.db.get_latest_price_date()
        if not last_date:
            return self._fetch_all_prices(days=750)

        start_date = (datetime.strptime(last_date, "%Y%m%d") + timedelta(days=1)).strftime("%Y%m%d")
        end_date = datetime.now().strftime("%Y%m%d")

        if start_date >= end_date:
            logger.info("   ℹ 行情已是最新")
            return {"records": 0, "message": "已是最新"}

        stocks = self.db.get_all_stocks()
        total = 0
        for i, stock in enumerate(stocks):
            code = stock["code"]
            try:
                df = self.pro.daily(ts_code=code, start_date=start_date, end_date=end_date)
                if df is not None and not df.empty:
                    prices = []
                    for _, row in df.iterrows():
                        prices.append({
                            "code": code,
                            "trade_date": row["trade_date"],
                            "open": row.get("open"),
                            "high": row.get("high"),
                            "low": row.get("low"),
                            "close": row.get("close"),
                            "volume": row.get("vol"),
                            "amount": row.get("amount"),
                            "pct_chg": row.get("pct_chg"),
                            "turnover": 0,
                        })
                    self.db.upsert_prices(prices)
                    total += len(prices)

                time.sleep(RATE_LIMIT_DELAY)
            except Exception as e:
                logger.warning(f"   ⚠ {code}: {e}")
                time.sleep(1)

        logger.info(f"   ✅ 增量更新: {total} 条")
        return {"records": total}

    # ─── 财务数据 ─────────────────────────────────────────────────────
    def _fetch_financials(self) -> dict:
        """获取财务报表数据（利润表 + 资产负债表 + 现金流量表）"""
        logger.info("📊 获取财务数据...")
        stocks = self.db.get_all_stocks()

        # 获取最近 3 年的报告期
        periods = []
        for year in range(datetime.now().year - 3, datetime.now().year + 1):
            for q in ["0331", "0630", "0930", "1231"]:
                periods.append(f"{year}{q}")

        total = 0
        errors = 0
        for i, stock in enumerate(stocks):
            code = stock["code"]
            try:
                # 利润表
                income = self.pro.income(ts_code=code, fields=(
                    'ts_code,end_date,revenue,n_income,total_profit,'
                    'operate_profit,ebit'
                ))
                time.sleep(RATE_LIMIT_DELAY)

                # 资产负债表
                balance = self.pro.balancesheet(ts_code=code, fields=(
                    'ts_code,end_date,total_assets,total_liab,'
                    'total_hldr_eqy_exc_min_int'
                ))
                time.sleep(RATE_LIMIT_DELAY)

                # 现金流量表
                cashflow = self.pro.cashflow(ts_code=code, fields=(
                    'ts_code,end_date,n_cashflow_act,free_cashflow'
                ))
                time.sleep(RATE_LIMIT_DELAY)

                # 合并
                records = []
                if income is not None and not income.empty:
                    for _, row in income.iterrows():
                        ed = row["end_date"]
                        bal = None
                        cf = None
                        if balance is not None and not balance.empty:
                            match = balance[balance["end_date"] == ed]
                            if not match.empty:
                                bal = match.iloc[0]
                        if cashflow is not None and not cashflow.empty:
                            match = cashflow[cashflow["end_date"] == ed]
                            if not match.empty:
                                cf = match.iloc[0]

                        ta = bal["total_assets"] if bal is not None and "total_assets" in bal.index else None
                        tl = bal["total_liab"] if bal is not None and "total_liab" in bal.index else None
                        se = bal["total_hldr_eqy_exc_min_int"] if bal is not None and "total_hldr_eqy_exc_min_int" in bal.index else None
                        ocf = cf["n_cashflow_act"] if cf is not None and "n_cashflow_act" in cf.index else None
                        fcf = cf["free_cashflow"] if cf is not None and "free_cashflow" in cf.index else None

                        ni = row.get("n_income")
                        roe = (ni / se * 100) if ni and se and se != 0 else None

                        records.append({
                            "code": code,
                            "end_date": ed,
                            "revenue": row.get("revenue"),
                            "net_income": ni,
                            "gross_profit": row.get("total_profit"),
                            "total_assets": ta,
                            "total_liabilities": tl,
                            "shareholders_equity": se,
                            "operating_cash_flow": ocf,
                            "free_cash_flow": fcf,
                            "eps": None,
                            "bps": None,
                            "roe": round(roe, 2) if roe else None,
                            "extra_json": None,
                        })

                if records:
                    self.db.upsert_financials(records)
                    total += len(records)

                if (i + 1) % 20 == 0:
                    logger.info(f"   进度: {i+1}/{len(stocks)} 只")

            except Exception as e:
                errors += 1
                logger.warning(f"   ⚠ {code} 财务获取失败: {e}")
                time.sleep(2)

        logger.info(f"   ✅ 财务数据: {total} 条, {errors} 个错误")
        return {"records": total, "errors": errors}

    # ─── 估值指标 ─────────────────────────────────────────────────────
    def _fetch_indicators(self) -> dict:
        """获取最新的 PE/PB/PS 等估值指标（daily_basic）"""
        logger.info("📐 获取估值指标...")
        try:
            # Tushare daily_basic 支持按日期获取全市场指标
            trade_date = datetime.now().strftime("%Y%m%d")
            df = self.pro.daily_basic(
                trade_date=trade_date,
                fields='ts_code,pe,pe_ttm,pb,ps,ps_ttm,dv_ratio,dv_ttm,total_mv,circ_mv'
            )

            # 如果当天没数据（非交易日），往前查
            attempts = 0
            while (df is None or df.empty) and attempts < 10:
                trade_date = (datetime.strptime(trade_date, "%Y%m%d") - timedelta(days=1)).strftime("%Y%m%d")
                df = self.pro.daily_basic(trade_date=trade_date, fields='ts_code,pe,pe_ttm,pb,ps,ps_ttm,dv_ratio,dv_ttm,total_mv,circ_mv')
                attempts += 1
                time.sleep(RATE_LIMIT_DELAY)

            if df is None or df.empty:
                return {"count": 0, "error": "无法获取估值指标"}

            records = []
            for _, row in df.iterrows():
                records.append({
                    "code": row["ts_code"],
                    "pe": row.get("pe"),
                    "pe_ttm": row.get("pe_ttm"),
                    "pb": row.get("pb"),
                    "ps": row.get("ps"),
                    "ps_ttm": row.get("ps_ttm"),
                    "dv_ratio": row.get("dv_ratio"),
                    "dv_ttm": row.get("dv_ttm"),
                    "total_mv": row.get("total_mv"),
                    "circ_mv": row.get("circ_mv"),
                })

            self.db.upsert_indicators(records)
            logger.info(f"   ✅ 估值指标: {len(records)} 只")
            return {"count": len(records), "trade_date": trade_date}

        except Exception as e:
            logger.error(f"估值指标获取失败: {e}")
            return {"count": 0, "error": str(e)}

    # ─── 沪深300基准 ────────────────────────────────────────────────────
    def _fetch_benchmark(self, days: int = 750) -> dict:
        """获取沪深300指数日线"""
        logger.info("📊 获取沪深300基准...")
        try:
            end_date = datetime.now().strftime("%Y%m%d")
            start_date = (datetime.now() - timedelta(days=int(days * 1.5))).strftime("%Y%m%d")

            df = self.pro.index_daily(
                ts_code='000300.SH',
                start_date=start_date,
                end_date=end_date
            )

            if df is None or df.empty:
                return {"count": 0, "error": "无法获取沪深300数据"}

            prices = []
            for _, row in df.iterrows():
                prices.append({
                    "code": "000300.SH",
                    "trade_date": row["trade_date"],
                    "open": row.get("open"),
                    "high": row.get("high"),
                    "low": row.get("low"),
                    "close": row.get("close"),
                    "volume": row.get("vol"),
                    "amount": row.get("amount"),
                    "pct_chg": row.get("pct_chg"),
                })

            self.db.upsert_benchmark(prices)
            logger.info(f"   ✅ 沪深300: {len(prices)} 条")
            return {"count": len(prices)}

        except Exception as e:
            logger.error(f"沪深300获取失败: {e}")
            return {"count": 0, "error": str(e)}
