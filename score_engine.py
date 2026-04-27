"""
RiskRadar v3 — 評分引擎
新增：
  - 所有新指標的評分函式
  - 趨勢分析（連續讀數）
  - 分數動能（Momentum）
  - 智慧行動建議（最佳操作策略文字）
  - 市場情境判斷（泡沫型/信用型/黑天鵝型）
"""

import json
from pathlib import Path
from datetime import datetime

DATA_DIR = Path(__file__).parent / "data"

# ═══════════════════════════════════════════
# 評分函式（0-100，越高越危險）
# ═══════════════════════════════════════════

# ── 估值類 ──
def s_cape(v):
    if v is None: return 50
    if v < 18: return 8
    if v < 22: return 18
    if v < 27: return 32
    if v < 32: return 52
    if v < 37: return 68
    if v < 42: return 82
    return 92

def s_buffett(v):
    if v is None: return 50
    if v < 80:  return 8
    if v < 100: return 20
    if v < 130: return 35
    if v < 160: return 55
    if v < 190: return 72
    if v < 220: return 85
    return 95

def s_margin_debt(v):
    if v is None: return 35
    if v < -20: return 8
    if v < 0:   return 20
    if v < 20:  return 35
    if v < 40:  return 55
    if v < 60:  return 72
    return 88

# ── 總體經濟類 ──
def s_yield_curve(bp):
    if bp is None: return 30
    if bp > 100:  return 5
    if bp > 50:   return 15
    if bp > 10:   return 28
    if bp > 0:    return 38
    if bp > -30:  return 55
    if bp > -60:  return 72
    if bp > -90:  return 82
    return 92

def s_yield_5y3m(bp):
    """5Y-3M 殖利率差（更早期的衰退訊號）"""
    if bp is None: return 30
    if bp > 50:   return 10
    if bp > 0:    return 30
    if bp > -30:  return 58
    if bp > -60:  return 74
    return 88

def s_sahm(v):
    if v is None: return 30
    if v < 0:    return 5
    if v < 0.2:  return 18
    if v < 0.35: return 38
    if v < 0.5:  return 58
    if v < 0.7:  return 78
    return 95

def s_nyfed_recession(v):
    """NY Fed 衰退機率（%）"""
    if v is None: return 30
    if v < 10:  return 12
    if v < 20:  return 28
    if v < 35:  return 48
    if v < 50:  return 65
    if v < 65:  return 78
    return 92

def s_stl_stress(v):
    """St. Louis 金融壓力指數（>1高壓，<-1低壓）"""
    if v is None: return 35
    if v < -1.5: return 10
    if v < -0.5: return 22
    if v < 0.5:  return 38
    if v < 1.0:  return 58
    if v < 1.5:  return 72
    return 88

def s_sloos(v):
    """SLOOS 放貸標準淨收緊（>0收緊，<0放寬）"""
    if v is None: return 35
    if v < -10: return 12
    if v < 0:   return 28
    if v < 10:  return 42
    if v < 25:  return 58
    if v < 40:  return 72
    return 88

def s_fed_qt(v):
    """Fed 縮表速度（月變化，十億美元）"""
    if v is None: return 30
    if v > 30:   return 10   # 擴表
    if v > 0:    return 25   # 停止縮表
    if v > -50:  return 38   # 緩慢縮表
    if v > -80:  return 52   # 正常縮表
    if v > -100: return 68   # 快速縮表
    return 82                # 超快速縮表（2022年情境）

# ── 信用/流動性類 ──
def s_hy_spread(bps):
    if bps is None: return 35
    if bps < 200: return 8
    if bps < 300: return 22
    if bps < 400: return 40
    if bps < 500: return 58
    if bps < 700: return 75
    return 92

def s_vix(v):
    if v is None: return 30
    if v < 12:  return 48   # 過低=過度自滿
    if v < 17:  return 18
    if v < 22:  return 30
    if v < 28:  return 48
    if v < 35:  return 65
    if v < 45:  return 80
    return 92

def s_vix_term(ratio):
    """VIX 期限結構比（VIX3M/VIX）"""
    if ratio is None: return 35
    if ratio > 1.15: return 12  # 市場平靜
    if ratio > 1.05: return 25
    if ratio > 0.95: return 42
    if ratio > 0.85: return 62
    if ratio > 0.75: return 78
    return 92  # <0.75 = 嚴重近期恐慌

def s_mortgage_delinquency(v):
    """房貸逾期率（%）"""
    if v is None: return 30
    if v < 1.5: return 10
    if v < 2.5: return 28
    if v < 3.5: return 48
    if v < 5.0: return 65
    if v < 7.0: return 80
    return 92

def s_cc_delinquency(v):
    """信用卡逾期率（%）"""
    if v is None: return 30
    if v < 2.0: return 10
    if v < 3.0: return 28
    if v < 4.0: return 48
    if v < 5.5: return 65
    if v < 7.0: return 80
    return 90

def s_dxy_trend(v):
    """DXY 月漲跌（正=美元升 = 對台股/新興市場不利）"""
    if v is None: return 30
    if v < -3:   return 12  # 美元大跌 = 風險偏好
    if v < -1:   return 22
    if v < 1:    return 35
    if v < 3:    return 52
    if v < 5:    return 68
    return 82

def s_money_market(chg_bn):
    """貨幣市場基金4週流入（十億）"""
    if chg_bn is None: return 35
    if chg_bn < -50:  return 12  # 資金離開避險
    if chg_bn < 0:    return 28
    if chg_bn < 50:   return 40
    if chg_bn < 100:  return 55
    if chg_bn < 200:  return 70
    return 85  # 大量資金逃往現金

def s_cross_asset_corr(spy_tlt_corr):
    """股債相關係數（正常負相關，危機時轉正）"""
    if spy_tlt_corr is None: return 30
    if spy_tlt_corr < -0.5: return 10  # 強負相關=正常
    if spy_tlt_corr < -0.2: return 22
    if spy_tlt_corr < 0.1:  return 40
    if spy_tlt_corr < 0.3:  return 62
    if spy_tlt_corr < 0.5:  return 78
    return 92  # 強正相關=流動性危機

# ── 法人/機構類 ──
def s_berkshire_cash(pct):
    if pct is None: return 40
    if pct < 10: return 10
    if pct < 15: return 22
    if pct < 20: return 38
    if pct < 25: return 58
    if pct < 30: return 75
    return 88

def s_defensive_flow(v):
    """防禦 vs 進攻資金流（正=資金流向防禦）"""
    if v is None: return 35
    if v < -3:   return 8   # 資金大量流向進攻
    if v < -1:   return 22
    if v < 0:    return 38
    if v < 1:    return 52
    if v < 3:    return 68
    return 82

def s_smallcap_relative(v):
    """小型股 vs 大型股相對強弱（1個月）"""
    if v is None: return 35
    if v > 3:   return 12   # 小型股大幅跑贏=廣度好
    if v > 1:   return 28
    if v > -1:  return 42
    if v > -3:  return 58
    if v > -5:  return 72
    return 85   # 小型股大幅跑輸=市場廣度極差

# ── 情緒類 ──
def s_fear_greed(v):
    if v is None: return 40
    if v < 20: return 18
    if v < 35: return 30
    if v < 50: return 45
    if v < 65: return 58
    if v < 80: return 72
    return 88

def s_vix_percentile(v):
    """VIX 歷史百分位（低百分位=低恐懼=潛在自滿風險）"""
    if v is None: return 30
    if v < 10:   return 52   # VIX極低=過度自滿
    if v < 25:   return 22
    if v < 50:   return 35
    if v < 75:   return 55
    if v < 90:   return 72
    return 88

# ── 台股專屬 ──
def s_foreign_futures(lots):
    if lots is None: return 40
    if lots > 25000:   return 8
    if lots > 10000:   return 18
    if lots > 0:       return 35
    if lots > -8000:   return 52
    if lots > -15000:  return 70
    if lots > -25000:  return 85
    return 95

def s_tw_business_lamp(lamp):
    return {"藍燈": 12, "黃藍燈": 28, "綠燈": 35,
            "黃紅燈": 62, "紅燈": 85}.get(lamp, 40)

def s_sox_vs_ma200(v):
    if v is None: return 40
    if v > 25:  return 15
    if v > 10:  return 28
    if v > 0:   return 40
    if v > -10: return 58
    if v > -20: return 75
    return 90

def s_smart_money(signal):
    """智慧錢信號（1=多 0=中性 -1=空）"""
    if signal == 1:  return 15
    if signal == 0:  return 40
    if signal == -1: return 75
    return 40

# ── 全球類 ──
def s_bdi_trend(v):
    """BDI 4週變化（%）"""
    if v is None: return 35
    if v > 15:   return 10
    if v > 5:    return 22
    if v > -5:   return 38
    if v > -15:  return 55
    if v > -25:  return 70
    return 85

def s_global_pmi(china, japan, germany):
    """全球 PMI 合成"""
    vals = [v for v in [china, japan, germany] if v is not None]
    if not vals: return 40
    avg = sum(vals) / len(vals)
    if avg > 55:   return 12
    if avg > 52:   return 25
    if avg > 50:   return 40
    if avg > 48:   return 58
    if avg > 46:   return 72
    return 88

def s_news_risk(v):
    if v is None: return 35
    return max(5, min(95, int(v)))

# ═══════════════════════════════════════════
# 條件式乘數（市場情境調整）
# ═══════════════════════════════════════════
def condition_multiplier(raw: dict) -> float:
    m = 1.0

    # 殖利率倒掛確認 → 其他指標敏感度上升
    if raw.get("yield_curve", 50) > 58:
        m += 0.10

    # NY Fed 衰退機率高 → 加重
    if raw.get("nyfed_recession", 30) > 60:
        m += 0.08

    # 金融壓力指數高 → 加重
    if raw.get("stl_stress", 35) > 65:
        m += 0.08

    # 銀行放貸收緊 + 信用卡逾期上升 → 2008型風險
    if raw.get("sloos", 35) > 60 and raw.get("cc_delinquency", 30) > 55:
        m += 0.12

    # Berkshire 過高 + CAPE 過高 → 估值泡沫共振
    if raw.get("berkshire_cash", 40) > 68 and raw.get("cape", 40) > 65:
        m += 0.08

    # 跨資產相關係數轉正 → 流動性危機訊號
    if raw.get("cross_asset_corr", 30) > 65:
        m += 0.10

    # VIX 期限結構倒掛 → 近期恐慌
    if raw.get("vix_term", 35) > 70:
        m += 0.06

    # 智慧錢出走 + 台指期大量空單 → 台股雙重壓力
    if raw.get("smart_money", 40) > 65 and raw.get("tw_foreign_futures", 40) > 70:
        m += 0.08

    return min(1.50, m)  # 最多放大 50%

# ═══════════════════════════════════════════
# 趨勢分析
# ═══════════════════════════════════════════
def analyze_trend(current_score: int, history: list) -> dict:
    """
    分析評分趨勢，判斷是否應該行動
    history：最近4次評分（含這次），從新到舊
    """
    if len(history) < 2:
        return {"trend": "insufficient_data", "action_signal": False, "momentum": 0}

    recent = history[:4]  # 最近4次
    momentum = recent[0] - recent[-1]  # 分數動能（正=上升趨勢）

    # 連續上升次數
    consec_up = 0
    for i in range(len(recent) - 1):
        if recent[i] > recent[i+1]:
            consec_up += 1
        else:
            break

    # 連續下降次數
    consec_down = 0
    for i in range(len(recent) - 1):
        if recent[i] < recent[i+1]:
            consec_down += 1
        else:
            break

    # 行動信號判斷
    action_signal = False
    action_reason = ""

    if current_score >= 80:
        action_signal = True
        action_reason = "單次達80分，立即啟動防禦"
    elif current_score >= 70 and consec_up >= 2:
        action_signal = True
        action_reason = f"連續{consec_up}次上升且超過70分"
    elif current_score >= 60 and consec_up >= 3:
        action_signal = True
        action_reason = f"連續{consec_up}次上升且超過60分"
    elif current_score >= 60 and momentum >= 10:
        action_signal = True
        action_reason = f"分數快速上升（+{momentum}分），觸發加速警示"

    trend_label = (
        "急速上升" if momentum >= 15 else
        "持續上升" if consec_up >= 2 else
        "緩慢上升" if momentum > 3 else
        "持續下降" if consec_down >= 2 else
        "緩慢下降" if momentum < -3 else "橫盤震盪"
    )

    return {
        "trend":         trend_label,
        "momentum":      momentum,
        "consec_up":     consec_up,
        "consec_down":   consec_down,
        "action_signal": action_signal,
        "action_reason": action_reason,
    }

# ═══════════════════════════════════════════
# 市場情境判斷
# ═══════════════════════════════════════════
def detect_market_regime(raw: dict, cats: dict) -> str:
    """
    判斷當前市場風險類型，幫助投資人理解風險來源
    """
    val_score    = cats.get("valuation", 40)
    credit_score = cats.get("credit", 40)
    macro_score  = cats.get("macro", 40)
    news_score   = cats.get("news", 40)

    # 估值泡沫型（2000、2022型）
    if val_score >= 70 and credit_score < 55:
        return "估值泡沫型"

    # 信用危機型（2008型）
    if credit_score >= 68 and raw.get("sloos", 35) > 60:
        return "信用危機型"

    # 總經衰退型
    if macro_score >= 65 and raw.get("sahm", 30) > 50:
        return "景氣衰退型"

    # 地緣政治/外部衝擊型
    if news_score >= 65 and val_score < 55 and macro_score < 55:
        return "地緣政治型"

    # 多重壓力型（最危險）
    if sum(1 for v in [val_score, credit_score, macro_score] if v >= 60) >= 2:
        return "多重壓力型"

    return "正常波動型"

# ═══════════════════════════════════════════
# 最佳操作策略文字生成
# ═══════════════════════════════════════════
def generate_action_strategy(score: int, trend_info: dict, regime: str,
                              cats: dict, raw: dict, tw_score: int) -> dict:
    """
    生成具體的操作策略建議
    """
    trend     = trend_info.get("trend", "")
    momentum  = trend_info.get("momentum", 0)
    act_signal= trend_info.get("action_signal", False)
    act_reason= trend_info.get("action_reason", "")

    # ── 倉位建議 ──
    if score <= 44:
        us_pos   = "70–80%（可積極）"
        tw_pos   = "70–80%（可積極）"
        cash_pos = "10–20%"
        phase    = "布局期"
    elif score <= 59:
        us_pos   = "60–70%（維持，暫停加碼）"
        tw_pos   = "60–70%（維持）"
        cash_pos = "20–30%"
        phase    = "觀察期"
    elif score <= 69:
        us_pos   = "45–60%（開始減碼）"
        tw_pos   = "50–60%（選擇性減碼）"
        cash_pos = "30–40%"
        phase    = "第一批減碼"
    elif score <= 79:
        us_pos   = "25–40%（加速減碼）"
        tw_pos   = "30–45%（保留台積電）"
        cash_pos = "45–60%"
        phase    = "第二批減碼"
    else:
        us_pos   = "10–20%（只保核心）"
        tw_pos   = "10–20%（只保台積電）"
        cash_pos = "70–80%"
        phase    = "防禦模式"

    # ── 核心建議文字 ──
    if score <= 44:
        core = (f"【{phase}】市場風險處於低水位，基本面與流動性均支撐多方。"
                f"可以按計劃積極布局，甚至在分數低於35時考慮加碼至目標上限。"
                f"建議優先布局：美股成長股（QQQ/科技ETF）、台積電及半導體供應鏈。")
    elif score <= 59:
        core = (f"【{phase}】風險開始醞釀，但尚未形成系統性威脅。"
                f"此時不宜加碼，維持現有部位即可。"
                f"重點觀察殖利率曲線走向和銀行放貸標準變化，若兩者同時惡化則提前進入減碼模式。")
    elif score <= 69:
        core = (f"【{phase}】多個核心指標同時警示，市場估值偏高且風險正在累積。"
                f"建議分批賣出：先減高本益比的成長股（美股QQQ、台股高本益比電子）20%，"
                f"保留防禦型（必需消費、醫療）和台積電等基本面強勁個股。"
                f"減碼動作分3-4週完成，不要一次全部執行。")
    elif score <= 79:
        core = (f"【{phase}】風險指標達歷史警戒水位。"
                f"歷史數據顯示，此區間後6-18個月內出現顯著跌幅機率超過70%。"
                f"建議大幅提高現金比例至50%以上，台股優先保留台積電、出清中小型電子。"
                f"美股移往現金、短期公債（SHY/BIL）或黃金（GLD）。")
    else:
        core = (f"【防禦模式】達到歷史最高風險區間。"
                f"歷史上此評分（{score}分）出現後，幾乎必跌。"
                f"建議將股票部位降至最低（10-20%只保台積電/SPY等核心）。"
                f"剩餘資金：50%持有現金，20%黃金（GLD），10%短期公債。"
                f"等待評分回落至45以下且殖利率曲線轉正，才開始分批回補。")

    # ── 趨勢補充 ──
    trend_note = ""
    if trend in ["急速上升", "持續上升"] and score >= 55:
        trend_note = f"⚠️ 趨勢警示：分數正在{trend}（動能+{momentum}），需要提前比建議更積極地執行減碼。"
    elif trend in ["持續下降", "緩慢下降"] and score >= 50:
        trend_note = f"📉 風險下降中（{trend}，動能{momentum}），可稍微放緩減碼步伐，等待確認趨勢反轉再行動。"

    # ── 情境說明 ──
    regime_note = {
        "估值泡沫型": "⚠️ 風險來源：市場估值嚴重偏高（非基本面惡化）。通常有足夠時間分批減碼，但不要等到基本面惡化才動。",
        "信用危機型": "🚨 風險來源：信用市場開始承壓（2008型）。此類危機傳導快，需要比平常更快速執行減碼。",
        "景氣衰退型": "📊 風險來源：景氣循環下行（Sahm Rule/LEI觸發）。通常有4-6個月的提前期，分批執行即可。",
        "地緣政治型": "🌍 風險來源：地緣政治不確定性。此類風險難以預測持續時間，建議先減碼台股（地緣敏感），美股可相對持有。",
        "多重壓力型": "🆘 風險來源：多重指標同時觸發。這是最危險的情況，建議比單一風險更快速執行防禦。",
        "正常波動型": "✅ 風險來源：正常市場波動。不需要特別行動，維持計劃即可。",
    }.get(regime, "")

    # ── 回補時機 ──
    if score >= 55:
        reentry = (
            "【回補時機】當系統評分連續3次讀數低於45分，"
            "且殖利率曲線轉為正值（10Y-2Y>0）、VIX回落至20以下，"
            "才開始第一批回補（目標倉位的1/3）。"
            "不要在市場「感覺」便宜時急著買，讓系統告訴你風險真的降低了。"
        )
    else:
        reentry = "【持續觀察】評分仍在正常範圍，無需考慮回補策略。"

    # ── 台股特別提示 ──
    tw_note = ""
    if tw_score >= 65:
        tw_note = "🇹🇼 台股特別注意：外資期貨淨空單或台幣快速貶值時，台股可能比美股更快下跌。優先減碼台股。"
    elif tw_score <= 35:
        tw_note = "🇹🇼 台股相對健康：M1b/M2 資金行情延續，台積電基本面支撐，台股可相對持有。"

    # ── 行動觸發判斷 ──
    action_trigger = ""
    if act_signal:
        action_trigger = f"🔔 行動觸發：{act_reason}。建議本週內開始執行減碼計劃。"
    else:
        needed_consec = 3 if score < 65 else 2 if score < 80 else 1
        curr_consec = trend_info.get("consec_up", 0)
        if curr_consec < needed_consec and score >= 55:
            still_need = needed_consec - curr_consec
            action_trigger = f"📋 尚未觸發：目前連續上升{curr_consec}次，還需{still_need}次連續上升才觸發行動訊號。"

    return {
        "phase":          phase,
        "core_strategy":  core,
        "trend_note":     trend_note,
        "regime":         regime,
        "regime_note":    regime_note,
        "reentry":        reentry,
        "tw_note":        tw_note,
        "action_trigger": action_trigger,
        "us_position":    us_pos,
        "tw_position":    tw_pos,
        "cash_position":  cash_pos,
        "top_risks": _get_top_risks(raw, cats),
    }

def _get_top_risks(raw, cats):
    risks = []
    if raw.get("yield_curve", 0) > 58:
        risks.append("殖利率曲線倒掛——衰退時鐘倒數中，歷史上12-18個月後衰退機率>80%")
    if raw.get("buffett", 0) > 72:
        risks.append("Buffett Indicator 過高——市場整體估值嚴重偏貴，安全邊際不足")
    if raw.get("berkshire_cash", 0) > 68:
        risks.append("Berkshire 現金創高——巴菲特找不到任何值得買的資產，是最強的估值警訊")
    if raw.get("stl_stress", 35) > 65:
        risks.append("St. Louis 金融壓力指數升高——金融市場整體壓力上升，流動性開始收縮")
    if raw.get("nyfed_recession", 30) > 55:
        risks.append("NY Fed 衰退機率偏高——模型預測未來12個月衰退風險顯著上升")
    if raw.get("sloos", 35) > 62:
        risks.append("銀行放貸標準收緊——信貸市場開始縮緊，企業融資成本上升")
    if raw.get("cross_asset_corr", 30) > 65:
        risks.append("股債黃金相關性轉正——正常負相關消失，可能是流動性危機前兆（2008型訊號）")
    if raw.get("tw_foreign_futures", 40) > 68:
        risks.append("外資台指期大量空單——外資正對台股進行系統性對沖，台股下行壓力大")
    if raw.get("news_risk", 35) > 65:
        risks.append("地緣政治/政策新聞風險升溫——市場情緒脆弱，任何衝擊都可能放大波動")
    if raw.get("vix_term", 35) > 72:
        risks.append("VIX 期限結構倒掛——市場近期恐慌高於中期，代表短期不確定性極高")
    return risks[:5]

# ═══════════════════════════════════════════
# 主評分函式
# ═══════════════════════════════════════════
def compute_score(data: dict) -> dict:
    macro   = data.get("macro", {})
    credit  = data.get("credit", {})
    val     = data.get("valuation", {})
    inst    = data.get("institutions", {})
    sent    = data.get("sentiment", {})
    tw      = data.get("taiwan", {})
    glo     = data.get("global_data", {})
    news    = data.get("news", {})

    raw = {
        # 估值
        "cape":          s_cape(val.get("cape")),
        "buffett":       s_buffett(val.get("buffett_indicator_pct")),
        "margin_debt":   s_margin_debt(val.get("margin_debt_yoy_pct")),

        # 總經
        "yield_curve":   s_yield_curve(macro.get("yield_curve_bp")),
        "yield_5y3m":    s_yield_5y3m(macro.get("yield_5y_3m_bp")),
        "sahm":          s_sahm(macro.get("sahm_rule")),
        "nyfed_recession": s_nyfed_recession(macro.get("nyfed_recession_prob_pct")),
        "stl_stress":    s_stl_stress(macro.get("stl_financial_stress")),
        "sloos":         s_sloos(macro.get("sloos_net_tightening_pct")),
        "fed_qt":        s_fed_qt(macro.get("fed_qt_monthly_bn")),

        # 信用
        "hy_spread":     s_hy_spread(credit.get("hy_spread_bps")),
        "vix":           s_vix(credit.get("vix")),
        "vix_term":      s_vix_term(credit.get("vix_term_structure_ratio")),
        "mortgage_delinquency": s_mortgage_delinquency(credit.get("mortgage_delinquency_pct")),
        "cc_delinquency": s_cc_delinquency(credit.get("credit_card_delinquency_pct")),
        "dxy_trend":     s_dxy_trend(credit.get("dxy_1m_pct")),
        "money_market":  s_money_market(credit.get("money_market_4w_chg_bn")),
        "cross_asset_corr": s_cross_asset_corr(inst.get("spy_tlt_corr_60d")),
        "gold_week":     50 if credit.get("gold_week_pct") is None else
                         max(10, min(88, int((credit["gold_week_pct"] + 5) * 6))),
        "copper_1m":     50 if credit.get("copper_1m_pct") is None else
                         max(10, min(88, int(50 - credit["copper_1m_pct"] * 4))),

        # 法人
        "berkshire_cash": s_berkshire_cash(inst.get("berkshire_cash_ratio_pct")),
        "defensive_flow": s_defensive_flow(inst.get("defensive_vs_offensive")),
        "smallcap_rel":   s_smallcap_relative(inst.get("iwm_spy_relative")),

        # 情緒
        "fear_greed":    s_fear_greed(sent.get("fear_greed_score")),
        "vix_pct":       s_vix_percentile(credit.get("vix_percentile_1y")),

        # 台股
        "tw_foreign_futures": s_foreign_futures(tw.get("foreign_futures_net_lots")),
        "tw_business_lamp":   s_tw_business_lamp(tw.get("business_cycle_lamp", "綠燈")),
        "sox_ma200":          s_sox_vs_ma200(tw.get("sox_vs_ma200")),
        "smart_money":        s_smart_money(tw.get("smart_money_signal")),

        # 全球
        "bdi_trend":     s_bdi_trend(glo.get("bdi_4w_chg_pct")),
        "global_pmi":    s_global_pmi(glo.get("china_caixin_pmi"),
                                      glo.get("japan_pmi"),
                                      glo.get("germany_pmi")),
        # 新聞
        "news_risk":     s_news_risk(news.get("news_risk_score")),
    }

    # 類別加權（重新設計，更精準）
    cats_cfg = {
        "valuation":   {"keys": ["cape", "buffett", "margin_debt"],
                        "wts":  [0.40, 0.40, 0.20], "cat_w": 0.18},
        "macro":       {"keys": ["yield_curve", "yield_5y3m", "sahm", "nyfed_recession",
                                  "stl_stress", "sloos", "fed_qt"],
                        "wts":  [0.22, 0.12, 0.18, 0.15, 0.13, 0.12, 0.08], "cat_w": 0.22},
        "credit":      {"keys": ["hy_spread", "vix", "vix_term", "mortgage_delinquency",
                                  "cc_delinquency", "dxy_trend", "money_market",
                                  "cross_asset_corr", "gold_week", "copper_1m"],
                        "wts":  [0.18, 0.12, 0.10, 0.12, 0.10, 0.08, 0.08, 0.12, 0.05, 0.05],
                        "cat_w": 0.20},
        "institution": {"keys": ["berkshire_cash", "defensive_flow", "smallcap_rel"],
                        "wts":  [0.45, 0.30, 0.25], "cat_w": 0.13},
        "sentiment":   {"keys": ["fear_greed", "vix_pct"],
                        "wts":  [0.60, 0.40], "cat_w": 0.08},
        "taiwan":      {"keys": ["tw_foreign_futures", "tw_business_lamp",
                                  "sox_ma200", "smart_money"],
                        "wts":  [0.35, 0.25, 0.25, 0.15], "cat_w": 0.10},
        "global":      {"keys": ["bdi_trend", "global_pmi"],
                        "wts":  [0.40, 0.60], "cat_w": 0.05},
        "news":        {"keys": ["news_risk"],
                        "wts":  [1.00], "cat_w": 0.04},
    }

    cats_scores = {}
    for cat, cfg in cats_cfg.items():
        s = sum(raw.get(k, 40) * w for k, w in zip(cfg["keys"], cfg["wts"]))
        cats_scores[cat] = round(s)

    # 台股獨立評分
    tw_raw = {k: raw[k] for k in ["tw_foreign_futures", "tw_business_lamp",
                                    "sox_ma200", "smart_money"]}
    tw_score = round(sum([
        tw_raw["tw_foreign_futures"] * 0.35,
        tw_raw["tw_business_lamp"]   * 0.25,
        tw_raw["sox_ma200"]          * 0.25,
        tw_raw["smart_money"]        * 0.15,
    ]))

    # 條件乘數
    mult = condition_multiplier(raw)
    base = sum(cats_scores[c] * cats_cfg[c]["cat_w"] for c in cats_cfg)
    final = min(100, round(base * mult))

    # 市場情境
    regime = detect_market_regime(raw, cats_scores)

    return {
        "score":            final,
        "tw_score":         tw_score,
        "base_score":       round(base),
        "multiplier":       round(mult, 2),
        "category_scores":  cats_scores,
        "raw_scores":       raw,
        "regime":           regime,
        "computed_at":      datetime.utcnow().isoformat() + "Z",
        "version":          "3.0",
    }

# ═══════════════════════════════════════════
# 主程式
# ═══════════════════════════════════════════
def main():
    data_path  = DATA_DIR / "market_data.json"
    score_path = DATA_DIR / "score.json"
    hist_path  = DATA_DIR / "score_history.json"

    if not data_path.exists():
        print("找不到 market_data.json")
        return

    with open(data_path, encoding="utf-8") as f:
        data = json.load(f)

    result = compute_score(data)

    # 讀歷史評分
    history = []
    if hist_path.exists():
        with open(hist_path, encoding="utf-8") as f:
            history = json.load(f)

    recent_scores = [h["score"] for h in history[-3:]][::-1]
    recent_scores.insert(0, result["score"])

    # 趨勢分析
    trend_info = analyze_trend(result["score"], recent_scores)
    result["trend"] = trend_info

    # 行動策略
    action = generate_action_strategy(
        result["score"], trend_info, result["regime"],
        result["category_scores"], result["raw_scores"],
        result["tw_score"]
    )
    result["action"] = action

    # 儲存評分
    with open(score_path, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    print(f"✅ 評分：{result['score']}/100｜台股：{result['tw_score']}/100｜{result['regime']}｜趨勢：{trend_info['trend']}")

    # 更新歷史
    history.append({
        "date":   result["computed_at"][:10],
        "time":   result["computed_at"][11:16],
        "score":  result["score"],
        "tw_score": result["tw_score"],
        "regime": result["regime"],
    })
    history = history[-120:]  # 保留120天

    with open(hist_path, "w", encoding="utf-8") as f:
        json.dump(history, f, ensure_ascii=False)

    return result

if __name__ == "__main__":
    main()
