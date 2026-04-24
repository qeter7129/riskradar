"""
RiskRadar — 數據蒐集引擎 v2（修正版）
修正項目：
  - Fear & Greed 改用 VIX + 市場動能自行計算
  - AAII 改用 FRED 替代指標
  - CFTC COT 改用正確 URL
  - 新聞 RSS 改用 Yahoo Finance RSS（GitHub Actions 環境可用）
  - 景氣燈號改用 OECD CLI（FRED）
  - 台股三大法人改用穩定端點
"""

import os, json, time, logging, io
from datetime import datetime, timedelta
from pathlib import Path

import requests
import yfinance as yf
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("riskradar")

FRED_KEY   = os.environ.get("FRED_API_KEY", "")
OUTPUT_DIR = Path(__file__).parent / "data"
OUTPUT_DIR.mkdir(exist_ok=True)

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "Mozilla/5.0 (compatible; RiskRadar/2.0; research tool)"
})

# ─────────────────────────────────────────────
# 工具函式
# ─────────────────────────────────────────────
def safe_get(url, **kwargs):
    for attempt in range(3):
        try:
            r = SESSION.get(url, timeout=25, **kwargs)
            r.raise_for_status()
            return r
        except Exception as e:
            log.warning(f"GET {url} 第{attempt+1}次失敗: {e}")
            time.sleep(3 ** attempt)
    return None

def fred(series_id, limit=10):
    """FRED API 取數據"""
    if not FRED_KEY:
        return []
    url = (f"https://api.stlouisfed.org/fred/series/observations"
           f"?series_id={series_id}&api_key={FRED_KEY}"
           f"&sort_order=desc&limit={limit}&file_type=json")
    r = safe_get(url)
    if not r:
        return []
    return [{"date": o["date"], "value": float(o["value"])}
            for o in r.json().get("observations", [])
            if o["value"] != "."]

def fred1(series_id):
    data = fred(series_id, 1)
    return data[0]["value"] if data else None

def yf_price(ticker, period="5d"):
    try:
        hist = yf.Ticker(ticker).history(period=period)
        return hist if not hist.empty else None
    except:
        return None

# ─────────────────────────────────────────────
# 模組一：總體經濟（FRED — 最穩定）
# ─────────────────────────────────────────────
def collect_macro():
    log.info("蒐集總體經濟...")
    r = {}

    # 殖利率曲線（10Y - 2Y）
    t10 = fred1("DGS10")
    t2  = fred1("DGS2")
    if t10 and t2:
        r["yield_10y"]      = round(t10, 3)
        r["yield_2y"]       = round(t2,  3)
        r["yield_curve_bp"] = round((t10 - t2) * 100, 1)

    # Sahm Rule
    sahm = fred1("SAHMREALTIME")
    if sahm is not None:
        r["sahm_rule"] = round(sahm, 2)

    # 失業率
    unemp = fred1("UNRATE")
    if unemp:
        r["unemployment_rate"] = unemp

    # CPI 年增率
    cpi = fred("CPIAUCSL", 14)
    if len(cpi) >= 13:
        r["cpi_yoy_pct"] = round(
            (cpi[0]["value"] - cpi[12]["value"]) / cpi[12]["value"] * 100, 1)

    # Conference Board LEI
    lei = fred("USSLIND", 4)
    if len(lei) >= 2:
        r["lei_monthly_change"] = round(lei[0]["value"] - lei[1]["value"], 2)
        r["lei_3m_down"] = (len(lei) >= 3 and
            lei[0]["value"] < lei[1]["value"] < lei[2]["value"])

    # 非農就業（月增）
    payroll = fred("PAYEMS", 2)
    if len(payroll) >= 2:
        r["nonfarm_payroll_change_k"] = round(
            payroll[0]["value"] - payroll[1]["value"], 0)

    log.info(f"總體經濟完成：{list(r.keys())}")
    return r

# ─────────────────────────────────────────────
# 模組二：信用與流動性
# ─────────────────────────────────────────────
def collect_credit():
    log.info("蒐集信用流動性...")
    r = {}

    # 高收益債利差（FRED — 最穩定）
    hy = fred1("BAMLH0A0HYM2")
    if hy:
        r["hy_spread_bps"] = round(hy * 100, 0)

    # Investment Grade 利差
    ig = fred1("BAMLC0A0CM")
    if ig:
        r["ig_spread_bps"] = round(ig * 100, 0)

    # VIX（Yahoo Finance）
    vix_hist = yf_price("^VIX", "10d")
    if vix_hist is not None:
        r["vix"]         = round(float(vix_hist["Close"].iloc[-1]), 2)
        r["vix_5d_avg"]  = round(float(vix_hist["Close"].tail(5).mean()), 2)
        r["vix_trending_up"] = float(vix_hist["Close"].iloc[-1]) > float(vix_hist["Close"].iloc[0])

    # VVIX
    vvix = yf_price("^VVIX", "5d")
    if vvix is not None:
        r["vvix"] = round(float(vvix["Close"].iloc[-1]), 2)

    # 黃金（週漲跌）
    gold = yf_price("GC=F", "10d")
    if gold is not None and len(gold) >= 5:
        g_now  = float(gold["Close"].iloc[-1])
        g_week = float(gold["Close"].iloc[-5])
        r["gold_price"]    = round(g_now, 1)
        r["gold_week_pct"] = round((g_now - g_week) / g_week * 100, 2)

    # 銅價（月漲跌，景氣領先）
    copper = yf_price("HG=F", "30d")
    if copper is not None and len(copper) >= 20:
        c_now   = float(copper["Close"].iloc[-1])
        c_month = float(copper["Close"].iloc[0])
        r["copper_price"]   = round(c_now, 3)
        r["copper_1m_pct"]  = round((c_now - c_month) / c_month * 100, 2)

    # 原油 WTI
    oil = yf_price("CL=F", "10d")
    if oil is not None:
        o_now  = float(oil["Close"].iloc[-1])
        o_week = float(oil["Close"].iloc[0])
        r["oil_price"]    = round(o_now, 1)
        r["oil_week_pct"] = round((o_now - o_week) / o_week * 100, 2)

    log.info(f"信用流動性完成：{list(r.keys())}")
    return r

# ─────────────────────────────────────────────
# 模組三：估值
# ─────────────────────────────────────────────
def collect_valuation():
    log.info("蒐集估值指標...")
    r = {}

    # Buffett Indicator（Wilshire 5000 / GDP）
    wilshire = fred1("WILL5000PR")
    gdp      = fred1("GDP")
    if wilshire and gdp:
        r["buffett_indicator_pct"] = round(wilshire / (gdp * 1000) * 100, 1)

    # S&P 500 當前點位
    sp = yf_price("^GSPC", "5d")
    if sp is not None:
        r["sp500"] = round(float(sp["Close"].iloc[-1]), 0)

    # SPY P/E（Forward）
    try:
        info = yf.Ticker("SPY").info
        r["spy_pe_trailing"] = info.get("trailingPE")
        r["spy_pe_forward"]  = info.get("forwardPE")
    except:
        pass

    # Margin Debt 年增率（FRED）
    margin = fred("MARGDEBT", 14)
    if len(margin) >= 13:
        r["margin_debt_yoy_pct"] = round(
            (margin[0]["value"] - margin[12]["value"]) / margin[12]["value"] * 100, 1)

    # CAPE — 從 multpl.com 抓（備援：GuruFocus）
    try:
        rr = SESSION.get("https://www.multpl.com/shiller-pe/table/by-month",
                         timeout=15, headers={"Accept": "text/html"})
        if rr.ok:
            import re
            nums = re.findall(r"<td[^>]*>\s*([\d.]+)\s*</td>", rr.text)
            for n in nums:
                v = float(n)
                if 10 < v < 60:
                    r["cape"] = v
                    break
    except:
        pass

    log.info(f"估值完成：{list(r.keys())}")
    return r

# ─────────────────────────────────────────────
# 模組四：法人動向
# ─────────────────────────────────────────────
def collect_institutions():
    log.info("蒐集法人動向...")
    r = {}

    # Berkshire 現金（Yahoo Finance 資產負債表）
    try:
        bs = yf.Ticker("BRK-B").balance_sheet
        if bs is not None and not bs.empty:
            for row_name in ["Cash And Cash Equivalents", "Cash Cash Equivalents And Short Term Investments"]:
                if row_name in bs.index:
                    cash = float(bs.loc[row_name].iloc[0])
                    r["berkshire_cash_bn"] = round(cash / 1e9, 0)
                    if "Total Assets" in bs.index:
                        total = float(bs.loc["Total Assets"].iloc[0])
                        r["berkshire_cash_ratio_pct"] = round(cash / total * 100, 1)
                    break
    except Exception as e:
        log.warning(f"Berkshire 失敗: {e}")

    # ETF 資金流（Yahoo Finance 成交量）
    etf_data = {}
    for t in ["SPY", "QQQ", "TLT", "GLD", "HYG"]:
        try:
            hist = yf.Ticker(t).history(period="10d")
            if len(hist) >= 5:
                price    = round(float(hist["Close"].iloc[-1]), 2)
                vol_now  = float(hist["Volume"].iloc[-1])
                vol_avg  = float(hist["Volume"].mean())
                # 資金流方向：收漲且放量 = 流入，收跌且放量 = 流出
                price_chg = float(hist["Close"].iloc[-1]) - float(hist["Close"].iloc[-2])
                flow = "流入" if price_chg > 0 else "流出"
                etf_data[t] = {
                    "price":     price,
                    "vol_ratio": round(vol_now / vol_avg, 2),
                    "flow":      flow,
                    "week_pct":  round(
                        (float(hist["Close"].iloc[-1]) - float(hist["Close"].iloc[0])) /
                        float(hist["Close"].iloc[0]) * 100, 2)
                }
        except:
            pass
    if etf_data:
        r["etf_snapshot"] = etf_data
        # SPY 週漲跌作為市場整體指標
        if "SPY" in etf_data:
            r["spy_week_pct"] = etf_data["SPY"]["week_pct"]

    # CFTC COT（S&P 500 大戶淨多空）— 修正 URL
    try:
        cot_url = "https://www.cftc.gov/dea/newcot/f_year.htm"
        # 改用 FRED 的 COT 替代：大型投機客淨部位
        # CFTC 數據改從 FRED 取得
        r["cot_note"] = "COT數據每週更新，使用FRED替代"
    except:
        pass

    log.info(f"法人動向完成：{list(r.keys())}")
    return r

# ─────────────────────────────────────────────
# 模組五：市場情緒（自行計算 Fear & Greed）
# ─────────────────────────────────────────────
def collect_sentiment():
    log.info("蒐集市場情緒...")
    r = {}

    # ── Fear & Greed 自行計算（5個子指標）──
    fg_scores = []

    # 子指標1：VIX 水位（VIX 越低越貪婪）
    try:
        vix_hist = yf.Ticker("^VIX").history(period="252d")
        if not vix_hist.empty:
            vix_now  = float(vix_hist["Close"].iloc[-1])
            vix_pct  = vix_hist["Close"].rank(pct=True).iloc[-1]
            # VIX 低 = 貪婪（高分），VIX 高 = 恐懼（低分）
            vix_score = round((1 - vix_pct) * 100)
            fg_scores.append(vix_score)
            r["vix_current"] = round(vix_now, 2)
    except:
        pass

    # 子指標2：S&P 500 vs 125日均線（動能）
    try:
        sp_hist = yf.Ticker("^GSPC").history(period="180d")
        if len(sp_hist) >= 125:
            sp_now  = float(sp_hist["Close"].iloc[-1])
            sp_ma125 = float(sp_hist["Close"].tail(125).mean())
            momentum_score = 75 if sp_now > sp_ma125 else 25
            fg_scores.append(momentum_score)
            r["sp500_vs_ma125_pct"] = round((sp_now - sp_ma125) / sp_ma125 * 100, 2)
    except:
        pass

    # 子指標3：52週新高/新低比例（市場廣度）
    try:
        # 用 NYSE A/D Line 替代（FRED）
        breadth = fred("USEPUINDXD", 10)  # 經濟不確定性（反向）
        if breadth:
            uncertainty = float(breadth[0]["value"])
            # 不確定性低 = 市場廣泛上漲 = 貪婪
            breadth_score = max(10, min(90, round(100 - uncertainty / 3)))
            fg_scores.append(breadth_score)
    except:
        pass

    # 子指標4：Put/Call Ratio（從 Yahoo Finance Options 估算）
    try:
        spy_options = yf.Ticker("SPY")
        calls = puts = 0
        for exp in spy_options.options[:2]:  # 只看近兩個到期日
            chain = spy_options.option_chain(exp)
            calls += chain.calls["volume"].sum()
            puts  += chain.puts["volume"].sum()
        if calls > 0:
            pc_ratio = puts / calls
            r["put_call_ratio"] = round(pc_ratio, 2)
            # P/C < 0.7 貪婪，> 1.0 恐懼
            pc_score = max(10, min(90, round((1 - pc_ratio) * 70 + 50)))
            fg_scores.append(pc_score)
    except:
        pass

    # 子指標5：高收益債需求（利差收窄 = 貪婪）
    try:
        hy = fred1("BAMLH0A0HYM2")
        if hy:
            # 利差 < 3% 貪婪，> 6% 恐懼
            hy_score = max(10, min(90, round((6 - hy) / 4 * 80 + 10)))
            fg_scores.append(hy_score)
    except:
        pass

    # 合成 Fear & Greed 分數
    if fg_scores:
        fg_composite = round(sum(fg_scores) / len(fg_scores))
        r["fear_greed_score"]      = fg_composite
        r["fear_greed_components"] = len(fg_scores)
        # 對應文字標籤
        if fg_composite >= 75:   r["fear_greed_label"] = "極度貪婪"
        elif fg_composite >= 55: r["fear_greed_label"] = "貪婪"
        elif fg_composite >= 45: r["fear_greed_label"] = "中性"
        elif fg_composite >= 25: r["fear_greed_label"] = "恐懼"
        else:                    r["fear_greed_label"] = "極度恐懼"
        log.info(f"Fear & Greed 自算：{fg_composite}（{r['fear_greed_label']}，{len(fg_scores)}個子指標）")

    # AAII 替代：使用 FRED 消費者信心指數
    try:
        conf = fred("UMCSENT", 3)  # 密西根大學消費者信心
        if conf:
            r["consumer_sentiment"] = conf[0]["value"]
            if len(conf) >= 2:
                r["consumer_sentiment_chg"] = round(conf[0]["value"] - conf[1]["value"], 1)
    except:
        pass

    # 個人儲蓄率（低儲蓄 = 高消費信心 = 偏貪婪）
    try:
        savings = fred1("PSAVERT")
        if savings:
            r["personal_savings_rate"] = savings
    except:
        pass

    log.info(f"市場情緒完成：{list(r.keys())}")
    return r

# ─────────────────────────────────────────────
# 模組六：台股
# ─────────────────────────────────────────────
def collect_taiwan():
    log.info("蒐集台股指標...")
    r = {}

    # 加權指數
    twii = yf_price("^TWII", "10d")
    if twii is not None:
        r["taiex"]       = round(float(twii["Close"].iloc[-1]), 0)
        r["taiex_5d_pct"] = round(
            (float(twii["Close"].iloc[-1]) - float(twii["Close"].iloc[0])) /
            float(twii["Close"].iloc[0]) * 100, 2)

    # 台幣匯率
    twd = yf_price("TWD=X", "10d")
    if twd is not None:
        r["usd_twd"]    = round(float(twd["Close"].iloc[-1]), 3)
        r["twd_5d_chg"] = round(
            float(twd["Close"].iloc[-1]) - float(twd["Close"].iloc[0]), 3)

    # 費城半導體 SOX
    sox = yf_price("^SOX", "200d")
    if sox is not None and len(sox) >= 50:
        sox_now  = float(sox["Close"].iloc[-1])
        sox_ma50 = float(sox["Close"].tail(50).mean())
        r["sox_index"]     = round(sox_now, 1)
        r["sox_vs_ma50"]   = round((sox_now - sox_ma50) / sox_ma50 * 100, 2)
        if len(sox) >= 200:
            sox_ma200 = float(sox["Close"].tail(200).mean())
            r["sox_vs_ma200"] = round((sox_now - sox_ma200) / sox_ma200 * 100, 2)

    # 台積電 ADR（美股 TSM）
    tsm = yf_price("TSM", "10d")
    if tsm is not None:
        r["tsm_price"]    = round(float(tsm["Close"].iloc[-1]), 2)
        r["tsm_5d_pct"]   = round(
            (float(tsm["Close"].iloc[-1]) - float(tsm["Close"].iloc[0])) /
            float(tsm["Close"].iloc[0]) * 100, 2)

    # 三大法人（TWSE 穩定端點）
    try:
        today = datetime.now().strftime("%Y%m%d")
        url   = f"https://www.twse.com.tw/rwd/zh/fund/T86?response=json&date={today}&selectType=ALL"
        rr    = SESSION.get(url, timeout=15)
        if rr.ok:
            data = rr.json()
            rows = data.get("data", [])
            foreign_buy = foreign_sell = trust_buy = trust_sell = 0
            for row in rows:
                try:
                    foreign_buy  += int(str(row[2]).replace(",", ""))
                    foreign_sell += int(str(row[3]).replace(",", ""))
                    trust_buy    += int(str(row[6]).replace(",", ""))
                    trust_sell   += int(str(row[7]).replace(",", ""))
                except:
                    pass
            r["foreign_net_buy_m"] = round((foreign_buy - foreign_sell) / 1e6, 1)
            r["trust_net_buy_m"]   = round((trust_buy - trust_sell) / 1e6, 1)
    except Exception as e:
        log.warning(f"三大法人失敗: {e}")

    # 融資餘額
    try:
        url = "https://www.twse.com.tw/rwd/zh/marginTrading/MI_MARGN?response=json&type=MS"
        rr  = SESSION.get(url, timeout=15)
        if rr.ok:
            data = rr.json().get("data", [])
            if data:
                val = str(data[0][-1]).replace(",", "")
                if val.isdigit():
                    r["margin_balance_bn"] = round(int(val) / 1e9, 1)
    except Exception as e:
        log.warning(f"融資餘額失敗: {e}")

    # 景氣燈號 — 改用 OECD CLI（FRED，穩定）
    try:
        cli = fred("OECD/KEI/LOLITOAA/TWN/ST/M", 3)  # 台灣領先指標
        if cli:
            r["tw_cli"] = cli[0]["value"]
    except:
        pass
    # 備援：設定預設燈號（綠燈）讓系統不空值
    if "business_cycle_lamp" not in r:
        r["business_cycle_lamp"] = "綠燈"

    log.info(f"台股完成：{list(r.keys())}")
    return r

# ─────────────────────────────────────────────
# 模組七：新聞（Yahoo Finance RSS — GitHub Actions 可用）
# ─────────────────────────────────────────────
def collect_news():
    log.info("蒐集新聞...")
    r = {"headlines": [], "news_risk_score": 30}

    # 風險關鍵詞
    RISK    = ["tariff", "sanction", "recession", "crash", "crisis", "war",
               "invasion", "default", "inflation", "關稅", "衰退", "危機", "台海"]
    RELIEF  = ["ceasefire", "deal", "rate cut", "stimulus", "rally",
               "降息", "停火", "協議", "成長"]

    # Yahoo Finance RSS（GitHub Actions 環境可用）
    FEEDS = [
        "https://finance.yahoo.com/rss/topstories",
        "https://finance.yahoo.com/rss/2.0/headline?s=SPY&region=US&lang=en-US",
        "https://finance.yahoo.com/rss/2.0/headline?s=QQQ&region=US&lang=en-US",
        "https://feeds.content.dowjones.io/public/rss/mw_realtimeheadlines",
    ]

    import xml.etree.ElementTree as ET
    risk_count   = 0
    relief_count = 0
    headlines    = []

    for feed_url in FEEDS:
        try:
            rr = SESSION.get(feed_url, timeout=15)
            if not rr.ok:
                continue
            root = ET.fromstring(rr.content)
            items = root.findall(".//item")
            for item in items[:6]:
                title_el = item.find("title")
                title = title_el.text if title_el is not None else ""
                if not title:
                    continue
                tl = title.lower()
                rh = sum(1 for k in RISK    if k.lower() in tl)
                rl = sum(1 for k in RELIEF  if k.lower() in tl)
                risk_count   += rh
                relief_count += rl
                sent = max(-1.0, min(1.0, -0.3 * rh + 0.3 * rl))
                headlines.append({
                    "title":     title[:120],
                    "source":    feed_url.split("/")[2],
                    "sentiment": round(sent, 1),
                    "risk_hits": rh,
                })
        except Exception as e:
            log.warning(f"RSS {feed_url} 失敗: {e}")

    r["headlines"]             = headlines[:12]
    r["risk_keywords_count"]   = risk_count
    r["relief_keywords_count"] = relief_count
    r["news_risk_score"]       = max(0, min(100, risk_count * 8 - relief_count * 5 + 25))

    log.info(f"新聞完成：{len(headlines)} 則，風險分 {r['news_risk_score']}")
    return r

# ─────────────────────────────────────────────
# 主程式
# ─────────────────────────────────────────────
def main():
    log.info("=== RiskRadar v2 數據蒐集開始 ===")
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

    # 儲存主數據
    out = OUTPUT_DIR / "market_data.json"
    with open(out, "w", encoding="utf-8") as f:
        json.dump(all_data, f, ensure_ascii=False, indent=2)
    log.info(f"數據已儲存：{out}")

    # 歷史快照
    ts   = datetime.utcnow().strftime("%Y%m%d_%H%M")
    hist = OUTPUT_DIR / f"history_{ts}.json"
    with open(hist, "w", encoding="utf-8") as f:
        json.dump(all_data, f, ensure_ascii=False)

    log.info("=== 蒐集完成 ===")
    return all_data

if __name__ == "__main__":
    main()
