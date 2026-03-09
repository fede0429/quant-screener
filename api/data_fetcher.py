"""
Tushare Pro 数据获取模块
负责从 Tushare API 拉取 A 股数据并存入 SQLite 缓存

新增: 带回调的方法用于后台刷新进度上报
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

    # ─── 核心刷新流程（旧接口，保留兼容） ──────────────────────────
    def refresh_all(self, full_refresh: bool = False) -> dict:
        """
        全量或增量刷新数据（同步，适合直接调用）
        """
        self._ensure_connected()
        stats = {}
        stats["stocks"] = self._fetch_stock_list()

        if full_refresh:
            stats["prices"] = self._fetch_all_prices(days=750)
        else:
            stats["prices"] = self._fetch_incremental_prices()

        if full_refresh:
            stats["financials"] = self._fetch_financials()

        stats["indicators"] = self._fetch_indicators(
            days=750 if full_refresh else 180,
            full_refresh=full_refresh
        )
        stats["benchmark"] = self._fetch_benchmark(days=750 if full_refresh else 60)
        self.db.save_setting("last_refresh", datetime.now().isoformat())
        return stats

    # ─── 带回调的分步方法（后台刷新用） ────────────────────────────
    def refresh_all_step_stocks(self) -> dict:
        """仅获取股票列表（第一步）"""
        self._ensure_connected()
        return self._fetch_stock_list()

    def fetch_all_prices_with_callback(self, days: int = 750, callback=None) -> dict:
        """全量拉取行情，带进度回调"""
        self._ensure_connected()
        logger.info(f"📈 全量获取日线行情（最近 {days} 天）...")
        stocks = self.db.get_all_stocks()
        end_date = datetime.now().strftime("%Y%m%d")
        start_date = (datetime.now() - timedelta(days=int(days * 1.5))).strftime("%Y%m%d")

        total_records = 0
        errors = 0
        n = len(stocks)
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
                    total_records += len(prices)

                if (i + 1) % 50 == 0:
                    logger.info(f"   进度: {i+1}/{n} 只, 共 {total_records} 条")
                    self.db.conn.commit()
                    if callback:
                        callback(i + 1, n, total_records)

                time.sleep(RATE_LIMIT_DELAY)

            except Exception as e:
                errors += 1
                logger.warning(f"   ⚠ {code} 行情获取失败: {e}")
                time.sleep(1)

        logger.info(f"   ✅ 行情获取完成: {total_records} 条, {errors} 个错误")
        return {"records": total_records, "errors": errors}

    def fetch_financials_with_callback(self, callback=None) -> dict:
        """获取财务数据，带进度回调"""
        self._ensure_connected()
        logger.info("📊 获取财务数据...")
        stocks = self.db.get_all_stocks()
        n = len(stocks)
        total = 0
        errors = 0

        for i, stock in enumerate(stocks):
            code = stock["code"]
            try:
                # 利润表
                income = self.pro.income(ts_code=code, fields=(
                    'ts_code,ann_date,f_ann_date,end_date,revenue,n_income,total_profit,'
                    'operate_profit,ebit'
                ))
                time.sleep(RATE_LIMIT_DELAY)

                # 资产负债表
                balance = self.pro.balancesheet(ts_code=code, fields=(
                    'ts_code,ann_date,f_ann_date,end_date,total_assets,total_liab,'
                    'total_hldr_eqy_exc_min_int'
                ))
                time.sleep(RATE_LIMIT_DELAY)

                # 现金流量表
                cashflow = self.pro.cashflow(ts_code=code, fields=(
                    'ts_code,ann_date,f_ann_date,end_date,n_cashflow_act,free_cashflow'
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
                        ann_date = row.get("ann_date")
                        f_ann_date = row.get("f_ann_date")
                        if not ann_date and bal is not None:
                            ann_date = bal.get("ann_date")
                        if not ann_date and cf is not None:
                            ann_date = cf.get("ann_date")
                        if not f_ann_date and bal is not None:
                            f_ann_date = bal.get("f_ann_date")
                        if not f_ann_date and cf is not None:
                            f_ann_date = cf.get("f_ann_date")

                        ni = row.get("n_income")
                        roe = (ni / se * 100) if ni and se and se != 0 else None

                        records.append({
                            "code": code,
                            "end_date": ed,
                            "ann_date": ann_date,
                            "f_ann_date": f_ann_date,
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
                    logger.info(f"   财务进度: {i+1}/{n} 只")
                    if callback:
                        callback(i + 1, n)

            except Exception as e:
                errors += 1
                logger.warning(f"   ⚠ {code} 财务获取失败: {e}")
                time.sleep(2)

        logger.info(f"   ✅ 财务数据: {total} 条, {errors} 个错误")
        return {"records": total, "errors": errors}

    # ─── 股票列表 ───────────────────────────────────────────────
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
            "乳制品": "消费", "免税": "消费", "零售": "消费", "纵织服装": "消费",
            "家居用品": "消费", "酥酒行业": "消费", "食品加工": "消费", "饮料制造": "消费",
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
            "黄金": "资源", "煎炭": "资源", "有色金属": "资源", "石油": "资源",
            "稀土": "资源", "钓铁": "资源", "采掘": "资源",
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

    # ─── 日线行情（旧接口） ─────────────────────────────────────
    def _fetch_all_prices(self, days: int = 750) -> dict:
        """全量拉取所有股票日线（分批）"""
        return self.fetch_all_prices_with_callback(days=days, callback=None)

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

    # ─── 财务数据（旧接口） ────────────────────────────────────
    def _fetch_financials(self) -> dict:
        """获取财务报表数据"""
        return self.fetch_financials_with_callback(callback=None)

    # ─── 估值指标 ───────────────────────────────────────────────
    def _fetch_indicators(self, days: int = 180, full_refresh: bool = False) -> dict:
        """Fetch historical daily_basic data and sync the latest indicator snapshot."""
        history_result = self._fetch_indicator_history(days=days, full_refresh=full_refresh)
        latest_trade_date = history_result.get("latest_trade_date") or self.db.get_latest_indicator_history_date()
        latest_records = history_result.get("latest_records") or []

        if not latest_records and latest_trade_date:
            latest_records = list(self.db.get_indicator_snapshot_map(latest_trade_date).values())

        if latest_records:
            self.db.upsert_indicators([
                {
                    "code": row["code"],
                    "pe": row.get("pe"),
                    "pe_ttm": row.get("pe_ttm"),
                    "pb": row.get("pb"),
                    "ps": row.get("ps"),
                    "ps_ttm": row.get("ps_ttm"),
                    "dv_ratio": row.get("dv_ratio"),
                    "dv_ttm": row.get("dv_ttm"),
                    "total_mv": row.get("total_mv"),
                    "circ_mv": row.get("circ_mv"),
                }
                for row in latest_records
            ])

        return {
            "count": len(latest_records),
            "trade_date": latest_trade_date,
            "history_rows": history_result.get("records", 0),
            "history_trade_dates": history_result.get("trade_dates", 0),
            "errors": history_result.get("errors", 0),
        }

    def _fetch_indicator_history(self, days: int = 180, full_refresh: bool = False) -> dict:
        self._ensure_connected()
        logger.info("📐 Fetching indicator history...")

        latest_history_date = None if full_refresh else self.db.get_latest_indicator_history_date()
        end_date = datetime.now().strftime("%Y%m%d")
        if latest_history_date:
            start_date = (datetime.strptime(latest_history_date, "%Y%m%d") + timedelta(days=1)).strftime("%Y%m%d")
        else:
            start_date = (datetime.now() - timedelta(days=int(days * 1.6))).strftime("%Y%m%d")

        if start_date > end_date:
            logger.info("   ℹ indicator history already up to date")
            return {
                "records": 0,
                "trade_dates": 0,
                "errors": 0,
                "latest_trade_date": latest_history_date,
                "latest_records": [],
            }

        trade_dates = self._get_open_trade_dates(start_date, end_date)
        if not trade_dates:
            return {
                "records": 0,
                "trade_dates": 0,
                "errors": 0,
                "latest_trade_date": latest_history_date,
                "latest_records": [],
            }

        total_records = 0
        error_count = 0
        successful_dates = 0
        latest_trade_date = latest_history_date
        latest_records = []
        for trade_date in trade_dates:
            try:
                df = self.pro.daily_basic(
                    trade_date=trade_date,
                    fields='ts_code,pe,pe_ttm,pb,ps,ps_ttm,dv_ratio,dv_ttm,total_mv,circ_mv'
                )
                if df is None or df.empty:
                    time.sleep(RATE_LIMIT_DELAY)
                    continue

                records = []
                for _, row in df.iterrows():
                    records.append({
                        "code": row["ts_code"],
                        "trade_date": trade_date,
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

                self.db.upsert_indicator_history(records)
                total_records += len(records)
                successful_dates += 1
                latest_trade_date = trade_date
                latest_records = records
                time.sleep(RATE_LIMIT_DELAY)

            except Exception as exc:
                error_count += 1
                logger.warning("   ⚠ indicator history failed for %s: %s", trade_date, exc)
                time.sleep(1)

        logger.info(
            "   ✅ indicator history: %s rows across %s trade dates",
            total_records,
            successful_dates,
        )
        return {
            "records": total_records,
            "trade_dates": successful_dates,
            "errors": error_count,
            "latest_trade_date": latest_trade_date,
            "latest_records": latest_records,
        }

    def _get_open_trade_dates(self, start_date: str, end_date: str) -> list[str]:
        df = self.pro.trade_cal(
            exchange='SSE',
            start_date=start_date,
            end_date=end_date,
            fields='cal_date,is_open'
        )
        if df is None or df.empty:
            return []

        result = []
        for _, row in df.iterrows():
            if str(row.get("is_open")) == "1":
                result.append(row["cal_date"])
        return sorted(result)

    # ─── 沪深300基准 ──────────────────────────────────────────────
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
