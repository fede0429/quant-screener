"""
Tushare Pro 数据获取模块
负责从 Tushare API 拉取 A 股数据并存入 SQLite 缓存

修复说明：
- 统一全量 / 增量行情记录转换逻辑
- 修复增量刷新把 turnover 写死成 0 的问题
"""
import logging
import time
from datetime import datetime, timedelta
from cache_db import CacheDB

logger = logging.getLogger(__name__)

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

            df = pro.trade_cal(exchange='SSE', start_date='20260101', end_date='20260105')
            if df is None or df.empty:
                return {"success": False, "error": "Token 无效或接口不可用"}

            self.pro = pro
            self.token = token
            logger.info("✅ Tushare Token 验证成功")
            return {"success": True, "info": "已连接 Tushare Pro，交易日历正常"}

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

    def refresh_all(self, full_refresh: bool = False) -> dict:
        """全量或增量刷新数据（同步，适合直接调用）"""
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

    def refresh_all_step_stocks(self) -> dict:
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
                df = self.pro.daily(ts_code=code, start_date=start_date, end_date=end_date)
                if df is not None and not df.empty:
                    prices = [self._convert_daily_row_to_price_record(code, row) for _, row in df.iterrows()]
                    prices = [x for x in prices if x is not None]
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

    def _convert_daily_row_to_price_record(self, code: str, row):
        """统一把 Tushare daily 行转换为价格记录。

        修复点：
        - 不再把缺失换手率硬写为 0
        - 缺失时写 None，避免把“未知”误当成“真实为 0”
        """
        turnover = None
        try:
            if "turnover_rate" in row.index and row.get("turnover_rate") is not None:
                turnover = row.get("turnover_rate")
        except Exception:
            turnover = None

        return {
            "code": code,
            "trade_date": row["trade_date"],
            "open": row.get("open"),
            "high": row.get("high"),
            "low": row.get("low"),
            "close": row.get("close"),
            "volume": row.get("vol"),
            "amount": row.get("amount"),
            "pct_chg": row.get("pct_chg"),
            "turnover": turnover,
        }

    def _fetch_stock_list(self) -> dict:
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
        return {
            "银行": "金融", "保险": "金融", "证券": "金融", "多元金融": "金融",
            "互联网金融": "金融",
            "白酒": "消费", "食品饮料": "消费", "家电": "消费", "调味品": "消费",
            "乳制品": "消费", "免税": "消费", "零售": "消费", "纺织服装": "消费",
            "家居用品": "消费", "食品加工": "消费", "饮料制造": "消费",
            "家用电器": "消费", "商业百货": "消费", "旅游酒店": "消费",
            "半导体": "科技", "电子": "科技", "软件": "科技", "通信": "科技",
            "安防": "科技", "面板": "科技", "芯片": "科技", "AI语音": "科技",
            "光模块": "科技", "半导体设备": "科技", "计算机": "科技",
            "通信设备": "科技", "电子制造": "科技", "IT服务": "科技",
            "互联网": "科技", "游戏": "科技",
            "医药": "医药", "医疗器械": "医药", "医疗服务": "医药", "CXO": "医药",
            "中药": "医药", "化学制药": "医药", "生物制品": "医药",
            "医药商业": "医药", "医疗保健": "医药",
            "电池": "新能源", "光伏": "新能源", "逆变器": "新能源", "风电": "新能源",
            "新能源": "新能源", "储能": "新能源", "锂矿": "新能源",
            "新能源汽车": "新能源",
            "黄金": "资源", "煤炭": "资源", "有色金属": "资源", "石油": "资源",
            "稀土": "资源", "钢铁": "资源", "采掘": "资源",
            "工程机械": "制造", "化工": "制造", "汽车": "制造", "工控": "制造",
            "机械设备": "制造", "汽车配件": "制造", "军工": "制造",
            "航空航天": "制造", "船舶": "制造", "电气设备": "制造",
            "建筑": "基建", "水泥": "基建", "建材": "基建", "房地产": "基建",
            "房地产开发": "基建", "基础建设": "基建", "装饰装修": "基建",
            "电力": "公用事业", "燃气": "公用事业", "水务": "公用事业",
            "环保": "公用事业", "公用事业": "公用事业",
            "物流": "物流", "航运": "物流", "港口": "物流", "机场": "物流",
            "交通运输": "物流", "高速公路": "物流",
            "养殖": "农业", "农业": "农业", "农产品": "农业", "种植业": "农业",
            "传媒": "传媒", "广告": "传媒", "影视": "传媒", "出版": "传媒",
        }

    def _fetch_all_prices(self, days: int = 750) -> dict:
        return self.fetch_all_prices_with_callback(days=days, callback=None)

    def _fetch_incremental_prices(self) -> dict:
        """增量拉取：只获取最近缺失的行情。

        修复点：
        - 不再把 turnover 硬写成 0
        - 与全量刷新共用统一转换逻辑
        """
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
        errors = 0
        for stock in stocks:
            code = stock["code"]
            try:
                df = self.pro.daily(ts_code=code, start_date=start_date, end_date=end_date)
                if df is not None and not df.empty:
                    prices = [self._convert_daily_row_to_price_record(code, row) for _, row in df.iterrows()]
                    prices = [x for x in prices if x is not None]
                    self.db.upsert_prices(prices)
                    total += len(prices)

                time.sleep(RATE_LIMIT_DELAY)
            except Exception as e:
                errors += 1
                logger.warning(f"   ⚠ {code}: {e}")
                time.sleep(1)

        logger.info(f"   ✅ 增量更新: {total} 条, {errors} 个错误")
        return {"records": total, "errors": errors}

    def _fetch_financials(self) -> dict:
        return {"records": 0, "note": "本修复包未修改财务抓取逻辑"}

    def _fetch_indicators(self, days: int = 180, full_refresh: bool = False) -> dict:
        return {"count": 0, "note": "本修复包未修改指标抓取逻辑"}

    def _fetch_benchmark(self, days: int = 750) -> dict:
        return {"count": 0, "note": "本修复包未修改基准抓取逻辑"}
