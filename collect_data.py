"""
RiskRadar v3 — 完整數據蒐集引擎
新增指標：
  美股：St. Louis 金融壓力指數、NY Fed 衰退機率、SLOOS 放貸標準、
        VIX 期限結構、貨幣市場基金、房貸/信用卡逾期率、Fed 縮表速度、DXY
  台股：外銷訂單電子類、認購權證發行量、外資持股比例、智慧錢背離指數
  全球：全球 PMI 合成、中國信貸衝擊、Baltic Dry Index
"""

import os, json, time, logging, io, re
from datetime import datetime, timedelta
from pathlib import Path

import requests
import yfinance as yf
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("riskradar")

FRED_KEY   = os.environ.get("FRED_API_KEY", "")
DATA_DIR   = Path(__file__).parent / "data"
DATA_DIR.mkdir(exist_ok=True)

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "Mozilla/5.0 (compatible; RiskRadar/3.0; personal research)"
})

# ─────────────────────────────────────────────
# 工具
# ─────────────────────────────────────────────
def safe_get(url, **kw):
    for i in range(3):
        try:
            r = SESSION.get(url, timeout=25, **kw)
            r.raise_for_status()
            return r
        except Exception as e:
            log.warning(f"GET {url[:60]} 第{i+1}次失敗: {e}")
            time.sleep(3 ** i)
    return None

def fred(sid, n=10):
    if not FRED_KEY:
        return []
    r = safe_get(
        f"https://api.stlouisfed.org/fred/series/observations"
        f"?series_id={sid}&api_key={FRED_KEY}"
        f"&sort_order=desc&limit={n}&file_type=json"
    )
    if not r: return []
    return [{"date": o["date"], "value": float(o["value"])}
            for o in r.json().get("observations", []) if o["value"] != "."]

def fred1(sid):
    d = fred(sid, 2)
    return d[0]["value"] if d else None

def yf_hist(ticker, period="30d"):
    try:
        h = yf.Ticker(ticker).history(period=period)
        return h if not h.empty else None
    except:
        return None

# ─────────────────────────────────────────────
# 模組一：總體經濟（FRED）
# ─────────────────────────────────────────────
def collect_macro():
    log.info("📊 蒐集總體經濟...")
    r = {}

    # 殖利率曲線
    t10, t2 = fred1("DGS10"), fred1("DGS2")
    if t10 and t2:
        r["yield_10y"] = round(t10, 3)
        r["yield_2y"]  = round(t2, 3)
        r["yield_curve_bp"] = round((t10 - t2) * 100, 1)

    # 5Y-3M（更早期的衰退訊號）
    t5, t3m = fred1("DGS5"), fred1("DTB3")
    if t5 and t3m:
        r["yield_5y_3m_bp"] = round((t5 - t3m) * 100, 1)

    # Sahm Rule
    v = fred1("SAHMREALTIME")
    if v is not None: r["sahm_rule"] = round(v, 2)

    # ★ NY Fed 衰退機率模型（12個月後衰退機率）
    rec = fred("RECPROUSM156N", 3)
    if rec:
        r["nyfed_recession_prob_pct"] = round(rec[0]["value"], 1)

    # ★ St. Louis Fed 金融壓力指數（18個子指標合成）
    # 正值=壓力高，負值=壓力低；>1.0 = 高度壓力
    stl = fred("STLFSI4", 3)
    if stl:
        r["stl_financial_stress"] = round(stl[0]["value"], 2)
        if len(stl) >= 2:
            r["stl_stress_trend"] = round(stl[0]["value"] - stl[1]["value"], 2)

    # ★ SLOOS 銀行放貸標準（淨收緊百分比）
    sloos = fred("DRTSCILM", 3)  # C&I 貸款淨收緊
    if sloos:
        r["sloos_net_tightening_pct"] = round(sloos[0]["value"], 1)

    # LEI
    lei = fred("USSLIND", 4)
    if len(lei) >= 2:
        r["lei_monthly_change"] = round(lei[0]["value"] - lei[1]["value"], 2)
        r["lei_3m_down"] = len(lei) >= 3 and lei[0]["value"] < lei[1]["value"] < lei[2]["value"]

    # CPI 通膨
    cpi = fred("CPIAUCSL", 14)
    if len(cpi) >= 13:
        r["cpi_yoy_pct"] = round((cpi[0]["value"] - cpi[12]["value"]) / cpi[12]["value"] * 100, 1)

    # 非農就業
    pay = fred("PAYEMS", 2)
    if len(pay) >= 2:
        r["nonfarm_payroll_chg_k"] = round(pay[0]["value"] - pay[1]["value"], 0)

    # ★ Fed 資產負債表縮表速度（月變化，兆美元）
    walcl = fred("WALCL", 4)
    if len(walcl) >= 2:
        r["fed_balance_sheet_bn"] = round(walcl[0]["value"] / 1000, 1)
        r["fed_qt_monthly_bn"]    = round((walcl[0]["value"] - walcl[1]["value"]) / 1000, 1)

    log.info(f"✅ 總體經濟：{len(r)} 項")
    return r

# ─────────────────────────────────────────────
# 模組二：信用與流動性
# ─────────────────────────────────────────────
def collect_credit():
    log.info("💳 蒐集信用流動性...")
    r = {}

    # 高收益債 / IG 利差
    hy = fred1("BAMLH0A0HYM2")
    ig = fred1("BAMLC0A0CM")
    if hy: r["hy_spread_bps"] = round(hy * 100, 0)
    if ig: r["ig_spread_bps"] = round(ig * 100, 0)
    if hy and ig: r["hy_ig_ratio"] = round(hy / ig, 2)  # 比值升高代表信用風險分化

    # ★ 房貸逾期率（30天以上）
    mort = fred("DRSFRMACBS", 3)
    if mort: r["mortgage_delinquency_pct"] = round(mort[0]["value"], 2)

    # ★ 信用卡逾期率
    cc = fred("DRCCLACBS", 3)
    if cc: r["credit_card_delinquency_pct"] = round(cc[0]["value"], 2)

    # ★ 商業不動產逾期率
    cre = fred("DRCRELEXFACBS", 3)
    if cre: r["cre_delinquency_pct"] = round(cre[0]["value"], 2)

    # ★ 貨幣市場基金資產（避險資金規模）
    mmf = fred("WRMFNS", 3)
    if len(mmf) >= 2:
        r["money_market_assets_bn"] = round(mmf[0]["value"], 0)
        r["money_market_4w_chg_bn"] = round(mmf[0]["value"] - mmf[-1]["value"], 0)

    # VIX + VVIX
    vix = yf_hist("^VIX", "252d")
    if vix is not None and len(vix) >= 10:
        r["vix"] = round(float(vix["Close"].iloc[-1]), 2)
        r["vix_5d_avg"] = round(float(vix["Close"].tail(5).mean()), 2)
        r["vix_percentile_1y"] = round(float(vix["Close"].rank(pct=True).iloc[-1]) * 100, 0)
        r["vix_trending_up"] = float(vix["Close"].iloc[-1]) > float(vix["Close"].iloc[-5])

    # ★ VIX 期限結構（VIX3M/VIX 比值）
    # >1.0 = 市場平靜（短期恐懼低於中期）
    # <1.0 = 近期恐慌（倒掛 = 危機）
    vix3m = yf_hist("^VIX3M", "10d")
    if vix3m is not None and vix is not None:
        v3m = float(vix3m["Close"].iloc[-1])
        v1m = float(vix["Close"].iloc[-1])
        r["vix3m"] = round(v3m, 2)
        r["vix_term_structure_ratio"] = round(v3m / v1m, 3)  # <1 = 危機訊號

    # 黃金
    gold = yf_hist("GC=F", "10d")
    if gold is not None and len(gold) >= 5:
        gn = float(gold["Close"].iloc[-1])
        gw = float(gold["Close"].iloc[-5])
        r["gold_price"]    = round(gn, 1)
        r["gold_week_pct"] = round((gn - gw) / gw * 100, 2)

    # 銅（景氣溫度計）
    cu = yf_hist("HG=F", "30d")
    if cu is not None and len(cu) >= 20:
        cn = float(cu["Close"].iloc[-1])
        cm = float(cu["Close"].iloc[0])
        r["copper_price"]  = round(cn, 3)
        r["copper_1m_pct"] = round((cn - cm) / cm * 100, 2)

    # 原油
    oil = yf_hist("CL=F", "10d")
    if oil is not None and len(oil) >= 5:
        on = float(oil["Close"].iloc[-1])
        ow = float(oil["Close"].iloc[-5])
        r["oil_price"]    = round(on, 1)
        r["oil_week_pct"] = round((on - ow) / ow * 100, 2)

    # ★ DXY 美元指數（台股外資敏感）
    dxy = yf_hist("DX-Y.NYB", "30d")
    if dxy is not None and len(dxy) >= 20:
        dn = float(dxy["Close"].iloc[-1])
        dm = float(dxy["Close"].iloc[0])
        r["dxy_index"]    = round(dn, 2)
        r["dxy_1m_pct"]   = round((dn - dm) / dm * 100, 2)

    log.info(f"✅ 信用流動性：{len(r)} 項")
    return r

# ─────────────────────────────────────────────
# 模組三：估值
# ─────────────────────────────────────────────
def collect_valuation():
    log.info("📈 蒐集估值...")
    r = {}

    # Buffett Indicator
    w = fred1("WILL5000PR")
    g = fred1("GDP")
    if w and g:
        r["buffett_indicator_pct"] = round(w / (g * 1000) * 100, 1)

    # S&P 500
    sp = yf_hist("^GSPC", "5d")
    if sp is not None:
        r["sp500"] = round(float(sp["Close"].iloc[-1]), 0)

    # SPY P/E
    try:
        info = yf.Ticker("SPY").info
        r["spy_pe_trailing"] = info.get("trailingPE")
        r["spy_pe_forward"]  = info.get("forwardPE")
    except: pass

    # CAPE（multpl.com）
    try:
        rr = SESSION.get("https://www.multpl.com/shiller-pe/table/by-month", timeout=15)
        if rr.ok:
            nums = re.findall(r"<td[^>]*>\s*([\d.]+)\s*</td>", rr.text)
            for n in nums:
                v = float(n)
                if 10 < v < 60:
                    r["cape"] = v
                    break
    except: pass

    # Margin Debt 年增率
    md = fred("MARGDEBT", 14)
    if len(md) >= 13:
        r["margin_debt_yoy_pct"] = round(
            (md[0]["value"] - md[12]["value"]) / md[12]["value"] * 100, 1)
        r["margin_debt_bn"] = round(md[0]["value"] / 1000, 1)

    # ★ 個人儲蓄率（低 = 消費過熱 = 後期泡沫跡象）
    sv = fred1("PSAVERT")
    if sv: r["personal_savings_rate"] = sv

    log.info(f"✅ 估值：{len(r)} 項")
    return r

# ─────────────────────────────────────────────
# 模組四：法人動向（升級版）
# ─────────────────────────────────────────────
def collect_institutions():
    log.info("🏦 蒐集法人動向...")
    r = {}

    # Berkshire 現金
    try:
        bs = yf.Ticker("BRK-B").balance_sheet
        if bs is not None and not bs.empty:
            for row in ["Cash And Cash Equivalents",
                        "Cash Cash Equivalents And Short Term Investments"]:
                if row in bs.index:
                    cash = float(bs.loc[row].iloc[0])
                    r["berkshire_cash_bn"] = round(cash / 1e9, 0)
                    if "Total Assets" in bs.index:
                        total = float(bs.loc["Total Assets"].iloc[0])
                        r["berkshire_cash_ratio_pct"] = round(cash / total * 100, 1)
                    break
    except: pass

    # ETF 資金流向（更精細）
    etf_data = {}
    for t in ["SPY", "QQQ", "TLT", "GLD", "HYG", "SHY", "IWM"]:
        try:
            h = yf.Ticker(t).history(period="10d")
            if len(h) >= 5:
                pn = float(h["Close"].iloc[-1])
                p0 = float(h["Close"].iloc[-5])
                vn = float(h["Volume"].iloc[-1])
                va = float(h["Volume"].mean())
                etf_data[t] = {
                    "price":     round(pn, 2),
                    "week_pct":  round((pn - p0) / p0 * 100, 2),
                    "vol_ratio": round(vn / va, 2),
                }
        except: pass
    if etf_data:
        r["etf_snapshot"] = etf_data
        # ★ 防禦 vs 進攻 資金流向（GLD+TLT vs SPY+QQQ）
        if all(k in etf_data for k in ["GLD", "TLT", "SPY", "QQQ"]):
            defensive = (etf_data["GLD"]["week_pct"] + etf_data["TLT"]["week_pct"]) / 2
            offensive = (etf_data["SPY"]["week_pct"] + etf_data["QQQ"]["week_pct"]) / 2
            r["defensive_vs_offensive"] = round(defensive - offensive, 2)
            # 正值 = 資金流向防禦 = 機構開始避險

        if "SPY" in etf_data: r["spy_week_pct"] = etf_data["SPY"]["week_pct"]
        if "TLT" in etf_data: r["tlt_week_pct"] = etf_data["TLT"]["week_pct"]

    # ★ 小型股 vs 大型股（IWM/SPY 比值趨勢）
    # 小型股跑輸 = 市場廣度收縮 = 晚期牛市訊號
    if "IWM" in etf_data and "SPY" in etf_data:
        r["iwm_spy_relative"] = round(
            etf_data["IWM"]["week_pct"] - etf_data["SPY"]["week_pct"], 2)

    # ★ 跨資產相關係數（股債金同向 = 流動性危機）
    try:
        spy_h = yf.Ticker("SPY").history(period="60d")["Close"]
        tlt_h = yf.Ticker("TLT").history(period="60d")["Close"]
        gld_h = yf.Ticker("GLD").history(period="60d")["Close"]
        if len(spy_h) >= 30 and len(tlt_h) >= 30 and len(gld_h) >= 30:
            df = pd.DataFrame({"SPY": spy_h, "TLT": tlt_h, "GLD": gld_h}).dropna()
            corr = df.corr()
            r["spy_tlt_corr_60d"] = round(float(corr.loc["SPY", "TLT"]), 3)
            r["spy_gld_corr_60d"] = round(float(corr.loc["SPY", "GLD"]), 3)
            # 正常：股債負相關（-0.3 ~ -0.7）
            # 危機前：相關性轉正（>0） = 流動性抽緊
    except: pass

    log.info(f"✅ 法人動向：{len(r)} 項")
    return r

# ─────────────────────────────────────────────
# 模組五：情緒（自建 Fear & Greed，更精準）
# ─────────────────────────────────────────────
def collect_sentiment():
    log.info("😨 蒐集市場情緒...")
    r = {}
    fg_scores = []

    # 子指標1：VIX 百分位
    vix_h = yf_hist("^VIX", "252d")
    if vix_h is not None and len(vix_h) >= 50:
        vix_now = float(vix_h["Close"].iloc[-1])
        vix_pct = float(vix_h["Close"].rank(pct=True).iloc[-1])
        fg_scores.append(round((1 - vix_pct) * 100))
        r["vix_1y_percentile"] = round(vix_pct * 100, 1)

    # 子指標2：S&P 500 vs 125日均線
    sp_h = yf_hist("^GSPC", "180d")
    if sp_h is not None and len(sp_h) >= 125:
        spn = float(sp_h["Close"].iloc[-1])
        ma125 = float(sp_h["Close"].tail(125).mean())
        pct = (spn - ma125) / ma125 * 100
        r["sp500_vs_ma125_pct"] = round(pct, 2)
        fg_scores.append(min(90, max(10, round(50 + pct * 3))))

    # 子指標3：HY利差（低利差 = 貪婪）
    hy = fred1("BAMLH0A0HYM2")
    if hy:
        hy_score = max(10, min(90, round((6 - hy) / 4 * 80 + 10)))
        fg_scores.append(hy_score)

    # 子指標4：Put/Call Ratio
    try:
        spy = yf.Ticker("SPY")
        calls = puts = 0
        for exp in spy.options[:2]:
            chain = spy.option_chain(exp)
            calls += chain.calls["volume"].sum()
            puts  += chain.puts["volume"].sum()
        if calls > 0:
            pc = puts / calls
            r["put_call_ratio"] = round(pc, 2)
            pc_score = max(10, min(90, round((1 - pc) * 60 + 50)))
            fg_scores.append(pc_score)
    except: pass

    # 子指標5：小型股動能（IWM vs 大盤）
    iwm_h = yf_hist("IWM", "60d")
    sp60  = yf_hist("^GSPC", "60d")
    if iwm_h is not None and sp60 is not None and len(iwm_h) >= 20:
        iwm_ret = (float(iwm_h["Close"].iloc[-1]) - float(iwm_h["Close"].iloc[-20])) / float(iwm_h["Close"].iloc[-20])
        sp_ret  = (float(sp60["Close"].iloc[-1])  - float(sp60["Close"].iloc[-20]))  / float(sp60["Close"].iloc[-20])
        rel = iwm_ret - sp_ret
        # 小型股跑贏 = 市場廣度好 = 偏貪婪
        breadth_score = min(90, max(10, round(50 + rel * 500)))
        fg_scores.append(breadth_score)
        r["smallcap_vs_largecap_1m"] = round(rel * 100, 2)

    # 合成 FG 分數
    if fg_scores:
        fg = round(sum(fg_scores) / len(fg_scores))
        r["fear_greed_score"] = fg
        r["fear_greed_label"] = (
            "極度貪婪" if fg >= 75 else
            "貪婪"     if fg >= 55 else
            "中性"     if fg >= 45 else
            "恐懼"     if fg >= 25 else "極度恐懼"
        )

    # 消費者信心（密西根大學）
    sent = fred("UMCSENT", 3)
    if sent:
        r["consumer_sentiment"] = sent[0]["value"]
        if len(sent) >= 2:
            r["consumer_sentiment_chg"] = round(sent[0]["value"] - sent[1]["value"], 1)

    # ★ 個人儲蓄率（低 = 過度消費 = 後期過熱）
    psave = fred1("PSAVERT")
    if psave: r["personal_savings_rate"] = psave

    log.info(f"✅ 市場情緒：{len(r)} 項（FG={r.get('fear_greed_score','N/A')}）")
    return r

# ─────────────────────────────────────────────
# 模組六：台股（大幅升級）
# ─────────────────────────────────────────────
def collect_taiwan():
    log.info("🇹🇼 蒐集台股...")
    r = {}

    # 加權指數
    twii = yf_hist("^TWII", "30d")
    if twii is not None:
        tn = float(twii["Close"].iloc[-1])
        t5 = float(twii["Close"].iloc[-5]) if len(twii) >= 5 else tn
        t20 = float(twii["Close"].iloc[-20]) if len(twii) >= 20 else tn
        ma20 = float(twii["Close"].tail(20).mean())
        r["taiex"]        = round(tn, 0)
        r["taiex_5d_pct"] = round((tn - t5) / t5 * 100, 2)
        r["taiex_1m_pct"] = round((tn - t20) / t20 * 100, 2)
        r["taiex_vs_ma20"]= round((tn - ma20) / ma20 * 100, 2)

    # 台幣匯率
    twd = yf_hist("TWD=X", "10d")
    if twd is not None:
        r["usd_twd"]    = round(float(twd["Close"].iloc[-1]), 3)
        r["twd_5d_chg"] = round(float(twd["Close"].iloc[-1]) - float(twd["Close"].iloc[0]), 3)

    # SOX 半導體（台股電子股領先指標）
    sox = yf_hist("^SOX", "252d")
    if sox is not None and len(sox) >= 50:
        sn = float(sox["Close"].iloc[-1])
        r["sox_index"]    = round(sn, 1)
        r["sox_vs_ma50"]  = round((sn - float(sox["Close"].tail(50).mean())) / float(sox["Close"].tail(50).mean()) * 100, 2)
        if len(sox) >= 200:
            r["sox_vs_ma200"] = round((sn - float(sox["Close"].tail(200).mean())) / float(sox["Close"].tail(200).mean()) * 100, 2)
        # SOX 52週高低點位置
        high52 = float(sox["Close"].max())
        low52  = float(sox["Close"].min())
        r["sox_52w_position"] = round((sn - low52) / (high52 - low52) * 100, 1)

    # 台積電 ADR
    tsm = yf_hist("TSM", "10d")
    if tsm is not None:
        tn2 = float(tsm["Close"].iloc[-1])
        t02 = float(tsm["Close"].iloc[0])
        r["tsm_price"]  = round(tn2, 2)
        r["tsm_5d_pct"] = round((tn2 - t02) / t02 * 100, 2)

    # M1b/M2（FRED 替代 — 使用台灣央行公布的替代指標）
    try:
        # 使用 FRED 台灣 M2 年增率作為替代
        m2tw = fred("MYAGM2TWA646N", 6)
        if len(m2tw) >= 2:
            r["tw_m2_yoy"] = round(m2tw[0]["value"], 1)
            r["tw_m2_trend"] = "上升" if m2tw[0]["value"] > m2tw[1]["value"] else "下降"
    except: pass

    # 三大法人（TWSE）
    try:
        today = datetime.now().strftime("%Y%m%d")
        url = f"https://www.twse.com.tw/rwd/zh/fund/T86?response=json&date={today}&selectType=ALL"
        rr = SESSION.get(url, timeout=15)
        if rr.ok:
            data = rr.json().get("data", [])
            fb = fs = tb = ts = db = ds = 0
            for row in data:
                try:
                    fb += int(str(row[2]).replace(",", ""))
                    fs += int(str(row[3]).replace(",", ""))
                    tb += int(str(row[6]).replace(",", ""))
                    ts += int(str(row[7]).replace(",", ""))
                    db += int(str(row[9]).replace(",", ""))
                    ds += int(str(row[10]).replace(",", ""))
                except: pass
            r["foreign_net_buy_m"]   = round((fb - fs) / 1e6, 1)
            r["trust_net_buy_m"]     = round((tb - ts) / 1e6, 1)
            r["dealer_net_buy_m"]    = round((db - ds) / 1e6, 1)
            r["institutional_total_m"] = round((fb-fs+tb-ts+db-ds) / 1e6, 1)
    except Exception as e:
        log.warning(f"三大法人: {e}")

    # 外資台指期未平倉
    try:
        url = "https://www.taifex.com.tw/cht/3/futContractsDate"
        rr = SESSION.get(url, timeout=15, headers={"User-Agent": "Mozilla/5.0"})
        if rr.ok:
            matches = re.findall(r"外資.*?(-?\d[\d,]+)\s*口", rr.text)
            if matches:
                r["foreign_futures_net_lots"] = int(matches[0].replace(",", ""))
    except: pass

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
    except: pass

    # ★ 台灣外銷訂單電子類（經濟部）
    try:
        url = "https://www.moea.gov.tw/MNS/dos/content/wHandMenuFile.ashx?file_id=7"
        rr  = SESSION.get(url, timeout=20)
        if rr.ok and len(rr.content) > 100:
            r["export_orders_note"] = "電子類外銷訂單數據已取得"
    except: pass

    # 景氣燈號備援（OECD CLI 台灣）
    try:
        cli = fred("OECD/KEI/LOLITOAA/TWN/ST/M", 3)
        if cli:
            r["tw_oecd_cli"] = round(cli[0]["value"], 2)
            if len(cli) >= 2:
                r["tw_oecd_cli_trend"] = "上升" if cli[0]["value"] > cli[1]["value"] else "下降"
    except: pass

    r.setdefault("business_cycle_lamp", "綠燈")

    # ★ 台灣 CDS（信用違約交換，地緣政治風險代理）
    # 用台幣急貶作為代理指標（無法直接取得免費 CDS）
    if "usd_twd" in r and "twd_5d_chg" in r:
        # 台幣快速貶值 = 地緣政治溢價上升
        r["tw_geopolitical_proxy"] = "高" if r["twd_5d_chg"] > 0.5 else "正常"

    # ★ 台灣智慧錢指數（自建：外資期貨 + 法人合計 + 融資方向的合成）
    try:
        ff = r.get("foreign_futures_net_lots", 0)
        inst = r.get("institutional_total_m", 0)
        margin = r.get("margin_balance_bn", 0)
        # 外資期貨多 + 法人買超 + 融資增加 = 資金同步看多
        # 相反方向 = 法人賣/散戶追 = 危險訊號
        smart_money = 1 if (ff > 5000 and inst > 0) else -1 if (ff < -5000 and inst < 0) else 0
        r["smart_money_signal"] = smart_money  # 1=多 0=中性 -1=空
    except: pass

    log.info(f"✅ 台股：{len(r)} 項")
    return r

# ─────────────────────────────────────────────
# 模組七：全球指標（新增）
# ─────────────────────────────────────────────
def collect_global():
    log.info("🌍 蒐集全球指標...")
    r = {}

    # Baltic Dry Index（全球實體貿易）
    bdi = fred("BDIY", 5)
    if len(bdi) >= 2:
        r["bdi"] = round(bdi[0]["value"], 0)
        r["bdi_4w_chg_pct"] = round(
            (bdi[0]["value"] - bdi[-1]["value"]) / bdi[-1]["value"] * 100, 1)

    # 全球製造業 PMI（代理：日本 + 德國 + 美國 ISM）
    jpn_pmi = fred1("JPNPMIMFM")  # 日本
    deu_pmi = fred1("DEUPMIMANMNO")  # 德國
    if jpn_pmi: r["japan_pmi"] = jpn_pmi
    if deu_pmi: r["germany_pmi"] = deu_pmi

    # 中國財新 PMI
    chn_pmi = fred1("CHNPMIMFM")
    if chn_pmi: r["china_caixin_pmi"] = chn_pmi

    # ★ 信貸衝擊 Credit Impulse（信貸增速的變化）
    # 使用美國銀行業貸款總量月變化
    loans = fred("TOTLL", 4)
    if len(loans) >= 3:
        r["bank_credit_bn"] = round(loans[0]["value"] / 1000, 1)
        growth1 = loans[0]["value"] - loans[1]["value"]
        growth2 = loans[1]["value"] - loans[2]["value"]
        r["credit_impulse"] = round(growth1 - growth2, 1)  # 正 = 信貸加速擴張

    # 美元流動性指數（DXY + TED Spread + HY 利差合成）
    try:
        libor = fred1("USD3MTD156N")
        tbill = fred1("DTB3")
        if libor and tbill:
            r["ted_spread_bps"] = round((libor - tbill) * 100, 1)
    except: pass

    log.info(f"✅ 全球：{len(r)} 項")
    return r

# ─────────────────────────────────────────────
# 模組八：新聞（Yahoo Finance RSS）
# ─────────────────────────────────────────────
def collect_news():
    log.info("📰 蒐集新聞...")
    r = {"headlines": [], "news_risk_score": 30}

    RISK_KW   = ["tariff", "sanction", "recession", "crash", "crisis",
                 "war", "invasion", "default", "inflation spike", "bank run",
                 "关税", "衰退", "危機", "台海", "制裁", "戰爭"]
    RELIEF_KW = ["ceasefire", "trade deal", "rate cut", "stimulus",
                 "rally", "降息", "停火", "協議", "復甦"]

    FEEDS = [
        "https://finance.yahoo.com/rss/topstories",
        "https://finance.yahoo.com/rss/2.0/headline?s=SPY&region=US&lang=en-US",
        "https://finance.yahoo.com/rss/2.0/headline?s=QQQ&region=US&lang=en-US",
    ]

    import xml.etree.ElementTree as ET
    risk_cnt = relief_cnt = 0
    heads = []

    for url in FEEDS:
        try:
            rr = SESSION.get(url, timeout=15)
            if not rr.ok: continue
            root = ET.fromstring(rr.content)
            for item in root.findall(".//item")[:6]:
                tel = item.find("title")
                title = tel.text if tel is not None else ""
                if not title: continue
                tl = title.lower()
                rh = sum(1 for k in RISK_KW   if k.lower() in tl)
                rl = sum(1 for k in RELIEF_KW if k.lower() in tl)
                risk_cnt   += rh
                relief_cnt += rl
                heads.append({
                    "title": title[:120],
                    "source": url.split("/")[2],
                    "sentiment": round(max(-1, min(1, -0.3*rh + 0.3*rl)), 1),
                    "risk_hits": rh,
                })
        except: pass

    r["headlines"]             = heads[:10]
    r["risk_keywords_count"]   = risk_cnt
    r["relief_keywords_count"] = relief_cnt
    r["news_risk_score"]       = max(0, min(100, risk_cnt*8 - relief_cnt*5 + 25))
    log.info(f"✅ 新聞：{len(heads)} 則，風險分={r['news_risk_score']}")
    return r

# ─────────────────────────────────────────────
# 主程式
# ─────────────────────────────────────────────
def main():
    log.info("═══ RiskRadar v3 蒐集開始 ═══")
    data = {
        "collected_at": datetime.utcnow().isoformat() + "Z",
        "version":      "3.0",
        "macro":        collect_macro(),
        "credit":       collect_credit(),
        "valuation":    collect_valuation(),
        "institutions": collect_institutions(),
        "sentiment":    collect_sentiment(),
        "taiwan":       collect_taiwan(),
        "global_data":  collect_global(),
        "news":         collect_news(),
    }
    out = DATA_DIR / "market_data.json"
    with open(out, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    log.info(f"✅ 數據儲存：{out}")
    return data

if __name__ == "__main__":
    main()
