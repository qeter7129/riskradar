"""
RiskRadar — 免費數據蒐集引擎
資料來源：FRED API / SEC EDGAR / Yahoo Finance / CFTC / TWSE / TAIFEX
執行方式：python collect_data.py
"""

import os, json, time, zipfile, io, logging
from datetime import datetime, timedelta
from pathlib import Path

import requests
import yfinance as yf
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("riskradar")

FRED_API_KEY = os.environ.get("FRED_API_KEY", "")  # 免費申請：fred.stlouisfed.org/docs/api/api_key.html
OUTPUT_DIR   = Path(__file__).parent.parent / "data"
OUTPUT_DIR.mkdir(exist_ok=True)

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "RiskRadar/1.0 (personal research tool)"})

# ─────────────────────────────────────────────
# 工具函式
# ─────────────────────────────────────────────
def safe_get(url, **kwargs):
    """帶重試的 HTTP GET"""
    for attempt in range(3):
        try:
            r = SESSION.get(url, timeout=20, **kwargs)
            r.raise_for_status()
            return r
        except Exception as e:
            log.warning(f"GET {url} 第{attempt+1}次失敗: {e}")
            time.sleep(2 ** attempt)
    return None

def fred_series(series_id: str, limit: int = 10) -> list[dict]:
    """從 FRED 取得時序數據"""
    if not FRED_API_KEY:
        log.warning(f"FRED_API_KEY 未設定，跳過 {series_id}")
        return []
    url = (f"https://api.stlouisfed.org/fred/series/observations"
           f"?series_id={series_id}&api_key={FRED_API_KEY}"
           f"&sort_order=desc&limit={limit}&file_type=json")
    r = safe_get(url)
    if not r:
        return []
    obs = r.json().get("observations", [])
    return [{"date": o["date"], "value": float(o["value"])} 
            for o in obs if o["value"] != "."]

def latest_fred(series_id: str) -> float | None:
    """取得 FRED 最新單一數值"""
    data = fred_series(series_id, limit=1)
    return data[0]["value"] if data else None

# ─────────────────────────────────────────────
# 模組一：總體經濟指標（FRED）
# ─────────────────────────────────────────────
def collect_macro() -> dict:
    log.info("蒐集總體經濟指標...")
    result = {}

    # 殖利率曲線（10年 - 2年）
    t10 = latest_fred("DGS10")
    t2  = latest_fred("DGS2")
    if t10 and t2:
        spread = round((t10 - t2) * 100, 1)  # 轉成 bps
        result["yield_curve_bp"] = spread
        result["yield_10y"] = t10
        result["yield_2y"]  = t2

    # Sahm Rule 即時指數（FRED 直接提供）
    sahm = latest_fred("SAHMREALTIME")
    if sahm is not None:
        result["sahm_rule"] = round(sahm, 2)

    # ISM 製造業 PMI（FRED 代理：MANEMP 或用 NMF... PMI 需另外處理）
    # 使用 ISM Manufacturing PMI 替代指標：工廠新訂單
    new_orders = latest_fred("AMTMNO")  # 製造業新訂單（月，億美元）
    if new_orders:
        result["factory_orders_bn"] = round(new_orders / 1000, 1)

    # Conference Board LEI
    lei = fred_series("USSLIND", limit=4)  # 領先指標
    if len(lei) >= 2:
        lei_chg = round(lei[0]["value"] - lei[1]["value"], 2)
        result["lei_monthly_change"] = lei_chg
        result["lei_3m_down"] = all(
            lei[i]["value"] < lei[i+1]["value"] for i in range(min(3, len(lei)-1))
        )

    # M2 貨幣供給年增率
    m2 = fred_series("M2SL", limit=14)
    if len(m2) >= 13:
        yoy = (m2[0]["value"] - m2[12]["value"]) / m2[12]["value"] * 100
        result["m2_yoy_pct"] = round(yoy, 1)

    # 失業率
    unemp = latest_fred("UNRATE")
    if unemp:
        result["unemployment_rate"] = unemp

    # CPI 年增率（通膨）
    cpi = fred_series("CPIAUCSL", limit=14)
    if len(cpi) >= 13:
        cpi_yoy = (cpi[0]["value"] - cpi[12]["value"]) / cpi[12]["value"] * 100
        result["cpi_yoy_pct"] = round(cpi_yoy, 1)

    log.info(f"總體經濟指標完成：{list(result.keys())}")
    return result

# ─────────────────────────────────────────────
# 模組二：信用與流動性（FRED + Yahoo Finance）
# ─────────────────────────────────────────────
def collect_credit() -> dict:
    log.info("蒐集信用與流動性指標...")
    result = {}

    # 高收益債利差（ICE BofA HY OAS）
    hy = latest_fred("BAMLH0A0HYM2")
    if hy:
        result["hy_spread_bps"] = round(hy * 100, 0)  # 轉 bps

    # Investment Grade 利差
    ig = latest_fred("BAMLC0A0CM")
    if ig:
        result["ig_spread_bps"] = round(ig * 100, 0)

    # TED Spread（3個月 LIBOR - 3個月 T-bill）
    libor = latest_fred("USD3MTD156N")
    tbill = latest_fred("DTB3")
    if libor and tbill:
        result["ted_spread_bps"] = round((libor - tbill) * 100, 1)

    # VIX + VVIX（Yahoo Finance）
    try:
        vix_data  = yf.Ticker("^VIX").history(period="5d")
        vvix_data = yf.Ticker("^VVIX").history(period="5d")
        if not vix_data.empty:
            result["vix"] = round(float(vix_data["Close"].iloc[-1]), 2)
        if not vvix_data.empty:
            result["vvix"] = round(float(vvix_data["Close"].iloc[-1]), 2)
    except Exception as e:
        log.warning(f"VIX 取得失敗: {e}")

    # 黃金價格（避險需求）
    try:
        gold = yf.Ticker("GC=F").history(period="10d")
        if not gold.empty:
            gold_now   = float(gold["Close"].iloc[-1])
            gold_week  = float(gold["Close"].iloc[0])
            result["gold_price"]    = round(gold_now, 1)
            result["gold_week_pct"] = round((gold_now - gold_week) / gold_week * 100, 1)
    except Exception as e:
        log.warning(f"黃金數據失敗: {e}")

    # 銅價（景氣領先）
    try:
        copper = yf.Ticker("HG=F").history(period="30d")
        if len(copper) >= 20:
            result["copper_price"]   = round(float(copper["Close"].iloc[-1]), 3)
            result["copper_1m_pct"]  = round(
                (float(copper["Close"].iloc[-1]) - float(copper["Close"].iloc[0])) /
                float(copper["Close"].iloc[0]) * 100, 1)
    except Exception as e:
        log.warning(f"銅價數據失敗: {e}")

    # 原油（WTI）
    try:
        oil = yf.Ticker("CL=F").history(period="10d")
        if not oil.empty:
            oil_now  = float(oil["Close"].iloc[-1])
            oil_week = float(oil["Close"].iloc[0])
            result["oil_price"]    = round(oil_now, 1)
            result["oil_week_pct"] = round((oil_now - oil_week) / oil_week * 100, 1)
    except Exception as e:
        log.warning(f"原油數據失敗: {e}")

    log.info(f"信用流動性指標完成：{list(result.keys())}")
    return result

# ─────────────────────────────────────────────
# 模組三：估值指標（Yahoo Finance + 計算）
# ─────────────────────────────────────────────
def collect_valuation() -> dict:
    log.info("蒐集估值指標...")
    result = {}

    # S&P 500 市值（用 SPY 市值估算）
    try:
        spy  = yf.Ticker("^GSPC").history(period="5d")
        sp_now = float(spy["Close"].iloc[-1]) if not spy.empty else None
        result["sp500"] = round(sp_now, 2) if sp_now else None

        # Forward P/E（S&P 500 整體，用 SPY）
        spy_info = yf.Ticker("SPY").info
        result["spy_pe_trailing"] = spy_info.get("trailingPE")
        result["spy_pe_forward"]  = spy_info.get("forwardPE")
    except Exception as e:
        log.warning(f"S&P 500 估值失敗: {e}")

    # Buffett Indicator = 股市總市值 / GDP
    # 市值：Wilshire 5000 Total Market Index
    wilshire = latest_fred("WILL5000PR")
    gdp      = latest_fred("GDP")  # 季度名目GDP（兆美元）
    if wilshire and gdp:
        buffett = round(wilshire / (gdp * 1000) * 100, 1)  # GDP是兆，Wilshire是指數點
        result["buffett_indicator_pct"] = buffett

    # 10 年 CAPE（Shiller PE）— 從網路取得最新值
    try:
        # multpl.com 是公開數據，可爬取
        r = safe_get("https://www.multpl.com/shiller-pe/table/by-month",
                     headers={"Accept": "text/html"})
        if r:
            from html.parser import HTMLParser
            class TableParser(HTMLParser):
                def __init__(self):
                    super().__init__()
                    self.in_td = False
                    self.cells = []
                    self.current = ""
                def handle_starttag(self, tag, attrs):
                    if tag == "td": self.in_td = True
                def handle_endtag(self, tag):
                    if tag == "td":
                        self.cells.append(self.current.strip())
                        self.current = ""
                        self.in_td = False
                def handle_data(self, data):
                    if self.in_td: self.current += data
            p = TableParser(); p.feed(r.text)
            # cells: [date, value, date, value, ...]
            if len(p.cells) >= 2:
                cape_str = p.cells[1].replace(",","").strip()
                result["cape"] = float(cape_str)
    except Exception as e:
        log.warning(f"CAPE 取得失敗: {e}")

    # Margin Debt（FINRA 融資餘額，FRED 系列）
    margin = fred_series("MARGDEBT", limit=14)
    if len(margin) >= 13:
        yoy = (margin[0]["value"] - margin[12]["value"]) / margin[12]["value"] * 100
        result["margin_debt_yoy_pct"] = round(yoy, 1)
        result["margin_debt_bn"]      = round(margin[0]["value"] / 1000, 1)

    log.info(f"估值指標完成：{list(result.keys())}")
    return result

# ─────────────────────────────────────────────
# 模組四：法人動向（SEC EDGAR + CFTC）
# ─────────────────────────────────────────────
def collect_institutions() -> dict:
    log.info("蒐集法人動向...")
    result = {}

    # Berkshire Hathaway 現金部位（最新 13F 財報）
    try:
        berk_info = yf.Ticker("BRK-B").info
        # 從資產負債表估算現金比
        berk_fin = yf.Ticker("BRK-B").balance_sheet
        if berk_fin is not None and not berk_fin.empty:
            if "Cash And Cash Equivalents" in berk_fin.index:
                cash = float(berk_fin.loc["Cash And Cash Equivalents"].iloc[0])
                total_assets = float(berk_fin.loc["Total Assets"].iloc[0]) if "Total Assets" in berk_fin.index else None
                if total_assets:
                    result["berkshire_cash_ratio_pct"] = round(cash / total_assets * 100, 1)
                result["berkshire_cash_bn"] = round(cash / 1e9, 0)
    except Exception as e:
        log.warning(f"Berkshire 數據失敗: {e}")

    # ETF 資金流向（SPY、QQQ、TLT、GLD、HYG）
    try:
        etf_flows = {}
        for ticker in ["SPY", "QQQ", "TLT", "GLD", "HYG"]:
            hist = yf.Ticker(ticker).history(period="10d")
            if len(hist) >= 5:
                vol_now  = float(hist["Volume"].iloc[-1])
                vol_avg  = float(hist["Volume"].mean())
                price    = float(hist["Close"].iloc[-1])
                # 成交量相對平均（>1.5 為異常放量）
                etf_flows[ticker] = {
                    "price": round(price, 2),
                    "vol_ratio": round(vol_now / vol_avg, 2)
                }
        result["etf_snapshot"] = etf_flows
    except Exception as e:
        log.warning(f"ETF 快照失敗: {e}")

    # CFTC COT 報告（S&P 500 E-mini 大戶部位）
    try:
        cot = _fetch_cot()
        if cot:
            result.update(cot)
    except Exception as e:
        log.warning(f"COT 報告失敗: {e}")

    # SEC Form 4 — 最近內部人交易
    try:
        insider = _fetch_insider_summary()
        if insider:
            result["insider_sell_buy_ratio"] = insider
    except Exception as e:
        log.warning(f"Insider 交易失敗: {e}")

    log.info(f"法人動向完成：{list(result.keys())}")
    return result

def _fetch_cot() -> dict | None:
    """下載 CFTC COT 報告，解析 S&P 500 大戶淨多空"""
    url = "https://www.cftc.gov/dea/newcot/FinComDat.zip"
    r = safe_get(url)
    if not r:
        return None
    with zipfile.ZipFile(io.BytesIO(r.content)) as zf:
        fname = [n for n in zf.namelist() if n.endswith(".txt") or n.endswith(".csv")]
        if not fname:
            return None
        with zf.open(fname[0]) as f:
            df = pd.read_csv(f, encoding="latin1")

    # 找 S&P 500 E-mini
    sp = df[df["Market_and_Exchange_Names"].str.contains("S&P 500", na=False, case=False)]
    if sp.empty:
        return None
    row = sp.iloc[0]
    lev_long  = int(row.get("Lev_Money_Positions_Long_All",  0))
    lev_short = int(row.get("Lev_Money_Positions_Short_All", 0))
    net = lev_long - lev_short
    return {
        "cot_sp500_lev_long":  lev_long,
        "cot_sp500_lev_short": lev_short,
        "cot_sp500_net":       net,
        "cot_date": str(row.get("As_of_Date_In_Form_YYYYMMDD", ""))
    }

def _fetch_insider_summary() -> float | None:
    """從 SEC EDGAR 取得近30天 Form 4，計算賣超/買超比"""
    url = ("https://efts.sec.gov/LATEST/search-index?q=%22form+4%22"
           "&dateRange=custom&startdt={}&enddt={}&_source=hits.hits._source.period_of_report"
           .format(
               (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d"),
               datetime.now().strftime("%Y-%m-%d")
           ))
    r = safe_get(url, headers={"User-Agent": "RiskRadar personal@example.com"})
    if not r:
        return None
    # 簡化：直接回傳搜索筆數（完整解析需 XML parsing）
    hits = r.json().get("hits", {}).get("total", {}).get("value", 0)
    return hits  # 實際部署可進一步解析 S/P 欄位

# ─────────────────────────────────────────────
# 模組五：市場情緒（AAII + Fear & Greed）
# ─────────────────────────────────────────────
def collect_sentiment() -> dict:
    log.info("蒐集市場情緒指標...")
    result = {}

    # Fear & Greed Index（CNN 非官方 API）
    try:
        r = safe_get("https://production.dataviz.cnn.io/index/fear-and-greed/graphdata/")
        if r:
            d = r.json()
            fng_now  = d.get("fear_and_greed", {}).get("score", None)
            fng_prev = d.get("fear_and_greed_historical", {}).get("data", [{}])[0].get("y", None)
            if fng_now is not None:
                result["fear_greed_score"] = round(float(fng_now), 1)
            if fng_prev is not None:
                result["fear_greed_prev_week"] = round(float(fng_prev), 1)
    except Exception as e:
        log.warning(f"Fear & Greed 失敗: {e}")

    # AAII 情緒（官網提供 XLS 下載）
    try:
        r = safe_get("https://www.aaii.com/sentimentsurvey/sent.xls")
        if r:
            df = pd.read_excel(io.BytesIO(r.content), skiprows=3)
            df.columns = [str(c).strip() for c in df.columns]
            df = df.dropna(subset=[df.columns[0]])
            if len(df) >= 1:
                latest = df.iloc[-1]
                bull_col  = [c for c in df.columns if "Bull" in c]
                bear_col  = [c for c in df.columns if "Bear" in c]
                neut_col  = [c for c in df.columns if "Neutral" in c]
                if bull_col:
                    result["aaii_bullish_pct"]  = round(float(latest[bull_col[0]]) * 100, 1)
                if bear_col:
                    result["aaii_bearish_pct"]  = round(float(latest[bear_col[0]]) * 100, 1)
                if neut_col:
                    result["aaii_neutral_pct"]  = round(float(latest[neut_col[0]]) * 100, 1)
    except Exception as e:
        log.warning(f"AAII 情緒失敗: {e}")

    # Put/Call Ratio（S&P 500 Options，CBOE 數據）
    try:
        r = safe_get("https://cdn.cboe.com/api/global/us_indices/daily_prices/PC_1_CBOE.json")
        if r:
            d = r.json()
            if "data" in d and d["data"]:
                latest_pc = d["data"][-1]
                result["put_call_ratio"] = round(float(latest_pc.get("value", 1.0)), 2)
    except Exception as e:
        log.warning(f"Put/Call Ratio 失敗: {e}")

    log.info(f"市場情緒指標完成：{list(result.keys())}")
    return result

# ─────────────────────────────────────────────
# 模組六：台股指標（TWSE + TAIFEX + 國發會）
# ─────────────────────────────────────────────
def collect_taiwan() -> dict:
    log.info("蒐集台股指標...")
    result = {}

    # 加權指數（Yahoo Finance）
    try:
        twii = yf.Ticker("^TWII").history(period="10d")
        if not twii.empty:
            result["taiex"] = round(float(twii["Close"].iloc[-1]), 0)
            result["taiex_5d_pct"] = round(
                (float(twii["Close"].iloc[-1]) - float(twii["Close"].iloc[0])) /
                float(twii["Close"].iloc[0]) * 100, 1)
    except Exception as e:
        log.warning(f"TAIEX 失敗: {e}")

    # 三大法人買賣超（TWSE JSON API）
    try:
        date_str = datetime.now().strftime("%Y%m%d")
        r = safe_get(f"https://www.twse.com.tw/rwd/zh/fund/TWT38U?response=json&date={date_str}")
        if r:
            d = r.json()
            if d.get("data"):
                # 取最新一筆（外資、投信、自營商）
                rows = d["data"]
                for row in rows[-3:]:
                    if "外陸資" in row[0] or "外資" in row[0]:
                        buy  = int(row[2].replace(",","")) if row[2].replace(",","").lstrip("-").isdigit() else 0
                        sell = int(row[3].replace(",","")) if row[3].replace(",","").lstrip("-").isdigit() else 0
                        result["foreign_net_buy_m"] = round((buy - sell) / 1e6, 1)
                    if "投信" in row[0]:
                        buy  = int(row[2].replace(",","")) if row[2].replace(",","").lstrip("-").isdigit() else 0
                        sell = int(row[3].replace(",","")) if row[3].replace(",","").lstrip("-").isdigit() else 0
                        result["trust_net_buy_m"] = round((buy - sell) / 1e6, 1)
    except Exception as e:
        log.warning(f"三大法人失敗: {e}")

    # 外資台指期未平倉（TAIFEX）
    try:
        r = safe_get("https://www.taifex.com.tw/cht/3/futContractsDate",
                     headers={"User-Agent": "Mozilla/5.0"})
        if r and "table" in r.text:
            # 簡單解析：找外資台指期淨部位數字
            import re
            text = r.text
            # 找「臺股期貨」區塊中的外資淨部位
            matches = re.findall(r"外資.*?(-?\d[\d,]+)\s*口", text)
            if matches:
                net = int(matches[0].replace(",", ""))
                result["foreign_futures_net_lots"] = net
    except Exception as e:
        log.warning(f"台指期外資部位失敗: {e}")

    # 融資餘額（TWSE）
    try:
        r = safe_get("https://www.twse.com.tw/rwd/zh/marginTrading/MI_MARGN?response=json")
        if r:
            d = r.json()
            if d.get("data") and len(d["data"]) > 0:
                # 第一列通常是整體融資餘額
                margin_row = d["data"][0]
                result["margin_balance_bn"] = round(int(str(margin_row[-1]).replace(",","")) / 1e9, 1)
    except Exception as e:
        log.warning(f"融資餘額失敗: {e}")

    # 台幣匯率（Yahoo Finance）
    try:
        twd = yf.Ticker("TWD=X").history(period="10d")
        if not twd.empty:
            result["usd_twd"] = round(float(twd["Close"].iloc[-1]), 3)
            result["twd_5d_chg"] = round(
                float(twd["Close"].iloc[-1]) - float(twd["Close"].iloc[0]), 3)
    except Exception as e:
        log.warning(f"台幣匯率失敗: {e}")

    # 台積電月營收年增率（公開觀測站，每月10日更新）
    try:
        r = safe_get(
            "https://mops.twse.com.tw/mops/web/ajax_t05st10_ifrs",
            params={"encodeURIComponent": 1, "step": 1, "firstin": 1,
                    "off": 1, "keyword4": "", "code1": "", "TYPEK2": "",
                    "checkbtn": "", "queryName": "co_id", "inpuType": "co_id",
                    "TYPEK": "all", "isnew": "false", "co_id": "2330",
                    "year": str(int(datetime.now().strftime("%Y")) - 1911),
                    "month": datetime.now().strftime("%m")},
            headers={"Referer": "https://mops.twse.com.tw/mops/web/t05st10_ifrs",
                     "User-Agent": "Mozilla/5.0"}
        )
        # 解析月營收（簡化）
        if r and "億" in r.text:
            import re
            nums = re.findall(r"([\d,]+\.\d+)\s*億", r.text)
            if nums:
                result["tsmc_monthly_rev_bn"] = float(nums[0].replace(",",""))
    except Exception as e:
        log.warning(f"台積電月營收失敗: {e}")

    # 景氣燈號（國發會）
    try:
        r = safe_get("https://www.ndc.gov.tw/en/cp.aspx?n=2860")
        # 取最新燈號分數（簡化爬取）
        if r:
            import re
            score_match = re.search(r"綜合判斷分數.*?(\d+)\s*分", r.text)
            lamp_match  = re.search(r"(紅燈|黃紅燈|綠燈|黃藍燈|藍燈)", r.text)
            if score_match:
                result["business_cycle_score"] = int(score_match.group(1))
            if lamp_match:
                result["business_cycle_lamp"] = lamp_match.group(1)
    except Exception as e:
        log.warning(f"景氣燈號失敗: {e}")

    # 費城半導體指數（SOX）— 台股領先指標
    try:
        sox = yf.Ticker("^SOX").history(period="30d")
        if not sox.empty:
            sox_now = float(sox["Close"].iloc[-1])
            ma200   = float(sox["Close"].tail(min(200, len(sox))).mean())
            result["sox_index"]      = round(sox_now, 1)
            result["sox_vs_ma200"]   = round((sox_now - ma200) / ma200 * 100, 1)
    except Exception as e:
        log.warning(f"SOX 失敗: {e}")

    log.info(f"台股指標完成：{list(result.keys())}")
    return result

# ─────────────────────────────────────────────
# 模組七：新聞情緒（RSS 聚合）
# ─────────────────────────────────────────────
def collect_news() -> dict:
    log.info("蒐集新聞RSS...")
    result = {"headlines": [], "risk_keywords_count": 0}

    RSS_FEEDS = [
        ("https://feeds.reuters.com/reuters/businessNews",    "Reuters"),
        ("https://apnews.com/rss",                            "AP"),
        ("https://www.federalreserve.gov/feeds/press_all.xml","Fed"),
        ("https://www.whitehouse.gov/feed/press-releases",    "WhiteHouse"),
        ("https://www.cna.com.tw/rss/aall.aspx",             "CNA"),
    ]

    RISK_KEYWORDS   = ["tariff", "sanction", "invasion", "default", "bank run",
                       "margin call", "recession", "關稅", "制裁", "衰退", "台海", "台灣海峽"]
    RELIEF_KEYWORDS = ["ceasefire", "trade deal", "rate cut", "stimulus", "停火", "降息"]

    risk_count   = 0
    relief_count = 0
    headlines    = []

    for feed_url, source in RSS_FEEDS:
        try:
            r = safe_get(feed_url)
            if not r:
                continue
            # 簡單 XML 解析（不用 feedparser 保持零依賴）
            import xml.etree.ElementTree as ET
            root = ET.fromstring(r.content)
            ns = {"atom": "http://www.w3.org/2005/Atom"}

            items = root.findall(".//item") or root.findall(".//atom:entry", ns)
            for item in items[:5]:  # 每個來源最多5則
                title_el = item.find("title") or item.find("atom:title", ns)
                title = title_el.text if title_el is not None else ""
                if not title:
                    continue

                # 關鍵詞計分
                title_lower = title.lower()
                risk_hits   = sum(1 for k in RISK_KEYWORDS   if k.lower() in title_lower)
                relief_hits = sum(1 for k in RELIEF_KEYWORDS if k.lower() in title_lower)
                risk_count   += risk_hits
                relief_count += relief_hits

                sentiment = -0.3 * risk_hits + 0.3 * relief_hits
                sentiment = max(-1.0, min(1.0, sentiment))

                headlines.append({
                    "title":     title[:120],
                    "source":    source,
                    "sentiment": round(sentiment, 1),
                    "risk_hits": risk_hits,
                })
        except Exception as e:
            log.warning(f"RSS {source} 解析失敗: {e}")

    # 新聞風險分（0-100）
    result["headlines"]            = headlines[:15]
    result["risk_keywords_count"]  = risk_count
    result["relief_keywords_count"]= relief_count
    raw_score = min(100, risk_count * 8 - relief_count * 5 + 30)
    result["news_risk_score"]      = max(0, raw_score)

    log.info(f"新聞蒐集完成：{len(headlines)} 則，風險分 {result['news_risk_score']}")
    return result

# ─────────────────────────────────────────────
# 主程式
# ─────────────────────────────────────────────
def main():
    log.info("=== RiskRadar 數據蒐集開始 ===")
    all_data = {
        "collected_at": datetime.utcnow().isoformat() + "Z",
        "macro":        collect_macro(),
        "credit":       collect_credit(),
        "valuation":    collect_valuation(),
        "institutions": collect_institutions(),
        "sentiment":    collect_sentiment(),
        "taiwan":       collect_taiwan(),
        "news":         collect_news(),
    }

    out_path = OUTPUT_DIR / "market_data.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(all_data, f, ensure_ascii=False, indent=2)
    log.info(f"數據已儲存至 {out_path}")

    # 同時儲存一份帶時間戳的歷史記錄
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M")
    hist_path = OUTPUT_DIR / f"history_{ts}.json"
    with open(hist_path, "w", encoding="utf-8") as f:
        json.dump(all_data, f, ensure_ascii=False)
    log.info(f"歷史記錄已儲存至 {hist_path}")

    log.info("=== 蒐集完成 ===")
    return all_data

if __name__ == "__main__":
    main()
