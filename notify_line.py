"""
RiskRadar — 評分引擎
將原始數據轉換成 0-100 風險指數，並產生條件式加權建議
"""

import json
from pathlib import Path
from datetime import datetime

DATA_DIR = Path(__file__).parent.parent / "data"

# ─────────────────────────────────────────────
# 單一指標評分函式（0-100，越高越危險）
# ─────────────────────────────────────────────

def score_cape(value) -> int:
    """Shiller CAPE：>30 警戒，>35 危險，>40 極危"""
    if value is None: return 50
    if value < 20: return 10
    if value < 25: return 25
    if value < 30: return 40
    if value < 35: return 62
    if value < 40: return 78
    return 90

def score_buffett(value) -> int:
    """Buffett Indicator (%)：>100 偏高，>150 泡沫化"""
    if value is None: return 50
    if value < 80:  return 10
    if value < 100: return 25
    if value < 120: return 40
    if value < 150: return 58
    if value < 175: return 74
    return 88

def score_yield_curve(bp) -> int:
    """殖利率倒掛 bps：< 0 即警訊，越負越危"""
    if bp is None: return 30
    if bp > 50:  return 5
    if bp > 0:   return 20
    if bp > -25: return 45
    if bp > -50: return 65
    if bp > -75: return 78
    return 90

def score_sahm(value) -> int:
    """Sahm Rule：>0.5 即衰退確認"""
    if value is None: return 30
    if value < 0:    return 5
    if value < 0.2:  return 20
    if value < 0.35: return 40
    if value < 0.5:  return 60
    if value < 0.7:  return 80
    return 95

def score_hy_spread(bps) -> int:
    """高收益債利差 bps"""
    if bps is None: return 35
    if bps < 250: return 10
    if bps < 350: return 25
    if bps < 450: return 45
    if bps < 550: return 65
    if bps < 700: return 80
    return 95

def score_vix(value) -> int:
    """VIX 恐慌指數"""
    if value is None: return 30
    if value < 12:  return 45   # 過低 = 過度自滿，也是警訊
    if value < 18:  return 20
    if value < 25:  return 35
    if value < 30:  return 55
    if value < 40:  return 72
    return 88

def score_vvix(value) -> int:
    """VVIX（波動率的波動率）"""
    if value is None: return 30
    if value < 80:  return 10
    if value < 95:  return 25
    if value < 110: return 40
    if value < 125: return 62
    if value < 140: return 78
    return 90

def score_berkshire_cash(pct) -> int:
    """Berkshire 現金佔資產比"""
    if pct is None: return 40
    if pct < 10: return 10
    if pct < 15: return 20
    if pct < 20: return 35
    if pct < 25: return 55
    if pct < 30: return 72
    return 85

def score_cot_net(net_lots) -> int:
    """COT 大戶淨多單（正=多，負=空）"""
    if net_lots is None: return 40
    if net_lots > 100000:  return 10
    if net_lots > 50000:   return 20
    if net_lots > 0:       return 35
    if net_lots > -50000:  return 55
    if net_lots > -100000: return 70
    return 85

def score_fear_greed(value) -> int:
    """Fear & Greed（0=恐懼，100=貪婪）"""
    if value is None: return 40
    if value < 20: return 20   # 極恐懼=低風險
    if value < 40: return 30
    if value < 60: return 45
    if value < 75: return 60
    if value < 85: return 72
    return 85

def score_aaii_bull(pct) -> int:
    """AAII 看多比例"""
    if pct is None: return 40
    if pct < 25: return 15
    if pct < 35: return 25
    if pct < 45: return 40
    if pct < 52: return 58
    if pct < 60: return 72
    return 85

def score_margin_debt_yoy(pct) -> int:
    """融資餘額年增率"""
    if pct is None: return 35
    if pct < -20: return 10
    if pct < 0:   return 25
    if pct < 20:  return 35
    if pct < 40:  return 52
    if pct < 60:  return 70
    return 85

def score_lei_change(monthly_change, three_month_down=False) -> int:
    """Conference Board LEI"""
    if monthly_change is None: return 40
    base = 0
    if three_month_down:   base += 30
    if monthly_change < -1: base += 30
    elif monthly_change < 0: base += 15
    elif monthly_change < 0.5: base += 5
    return min(90, max(10, 30 + base))

def score_news_risk(score) -> int:
    """新聞風險指數（直接使用計算值）"""
    if score is None: return 35
    return min(95, max(5, int(score)))

def score_gold_week(pct) -> int:
    """黃金週漲跌（急漲=避險需求升溫=風險訊號）"""
    if pct is None: return 30
    if pct < -3:  return 20
    if pct < 0:   return 25
    if pct < 2:   return 30
    if pct < 4:   return 45
    if pct < 6:   return 62
    return 78

def score_copper_1m(pct) -> int:
    """銅價月變化（下跌=需求萎縮=景氣警訊）"""
    if pct is None: return 35
    if pct > 5:   return 10
    if pct > 2:   return 20
    if pct > 0:   return 30
    if pct > -3:  return 45
    if pct > -7:  return 62
    return 80

def score_tw_foreign_futures(net_lots) -> int:
    """台指期外資淨部位"""
    if net_lots is None: return 40
    if net_lots > 20000:   return 10
    if net_lots > 5000:    return 20
    if net_lots > 0:       return 35
    if net_lots > -8000:   return 55
    if net_lots > -15000:  return 72
    return 88

def score_tw_business_lamp(lamp) -> int:
    """景氣燈號"""
    mapping = {"藍燈": 15, "黃藍燈": 30, "綠燈": 35, "黃紅燈": 65, "紅燈": 82}
    return mapping.get(lamp, 40)

def score_sox_vs_ma200(pct) -> int:
    """SOX vs 200MA（台股電子股的領先指標）"""
    if pct is None: return 40
    if pct > 20:   return 18
    if pct > 10:   return 28
    if pct > 0:    return 38
    if pct > -10:  return 55
    if pct > -20:  return 72
    return 88

# ─────────────────────────────────────────────
# 條件式乘數（市場情境調整）
# ─────────────────────────────────────────────

def compute_condition_multiplier(scores: dict) -> float:
    """
    條件式加權：
    當多個核心指標同時觸發，風險不是加法而是乘法
    例如：殖利率倒掛+CAPE高+信用利差擴張，三者共振時放大係數
    """
    multiplier = 1.0

    # 殖利率倒掛確認 → 其他指標門檻降低15%（風險放大）
    if scores.get("yield_curve", 0) > 60:
        multiplier += 0.12

    # Berkshire 現金過高 → 估值類指標放大
    if scores.get("berkshire_cash", 0) > 65:
        multiplier += 0.08

    # VIX 極低（過度自滿）+ 估值高 → 最危險組合
    if scores.get("vix", 50) < 30 and scores.get("cape", 0) > 55:
        multiplier += 0.10

    # COT 大戶轉空且外資台指期也轉空 → 台股雙重壓力
    if scores.get("cot_net", 0) > 60 and scores.get("tw_foreign_futures", 0) > 60:
        multiplier += 0.08

    # 新聞風險極高 → 情緒類指標敏感度提升
    if scores.get("news_risk", 0) > 70:
        multiplier += 0.06

    return min(1.4, multiplier)  # 最多放大 40%

# ─────────────────────────────────────────────
# 主評分函式
# ─────────────────────────────────────────────

def compute_full_score(data: dict) -> dict:
    """
    輸入：collect_data.py 產出的 JSON
    輸出：完整評分報告
    """
    macro    = data.get("macro",        {})
    credit   = data.get("credit",       {})
    valuation= data.get("valuation",    {})
    inst     = data.get("institutions", {})
    sentiment= data.get("sentiment",    {})
    taiwan   = data.get("taiwan",       {})
    news     = data.get("news",         {})

    # 各指標原始評分
    raw_scores = {
        # 估值
        "cape":          score_cape(valuation.get("cape")),
        "buffett":       score_buffett(valuation.get("buffett_indicator_pct")),
        "margin_debt":   score_margin_debt_yoy(valuation.get("margin_debt_yoy_pct")),

        # 總經
        "yield_curve":   score_yield_curve(macro.get("yield_curve_bp")),
        "sahm":          score_sahm(macro.get("sahm_rule")),
        "lei":           score_lei_change(macro.get("lei_monthly_change"),
                                          macro.get("lei_3m_down", False)),

        # 信用流動性
        "hy_spread":     score_hy_spread(credit.get("hy_spread_bps")),
        "vix":           score_vix(credit.get("vix")),
        "vvix":          score_vvix(credit.get("vvix")),
        "gold_week":     score_gold_week(credit.get("gold_week_pct")),
        "copper_1m":     score_copper_1m(credit.get("copper_1m_pct")),

        # 法人動向
        "berkshire_cash":score_berkshire_cash(inst.get("berkshire_cash_ratio_pct")),
        "cot_net":       score_cot_net(inst.get("cot_sp500_net")),

        # 情緒
        "fear_greed":    score_fear_greed(sentiment.get("fear_greed_score")),
        "aaii_bull":     score_aaii_bull(sentiment.get("aaii_bullish_pct")),

        # 新聞地緣
        "news_risk":     score_news_risk(news.get("news_risk_score")),

        # 台股專屬
        "tw_foreign_futures": score_tw_foreign_futures(taiwan.get("foreign_futures_net_lots")),
        "tw_business_lamp":   score_tw_business_lamp(taiwan.get("business_cycle_lamp")),
        "sox_ma200":     score_sox_vs_ma200(taiwan.get("sox_vs_ma200")),
    }

    # 類別加權
    category_weights = {
        "valuation": {
            "indices": ["cape", "buffett", "margin_debt"],
            "weights": [0.40, 0.40, 0.20],
            "cat_weight": 0.20,
        },
        "macro": {
            "indices": ["yield_curve", "sahm", "lei"],
            "weights": [0.45, 0.35, 0.20],
            "cat_weight": 0.22,
        },
        "credit": {
            "indices": ["hy_spread", "vix", "vvix", "gold_week", "copper_1m"],
            "weights": [0.35, 0.20, 0.15, 0.15, 0.15],
            "cat_weight": 0.18,
        },
        "institution": {
            "indices": ["berkshire_cash", "cot_net"],
            "weights": [0.50, 0.50],
            "cat_weight": 0.15,
        },
        "sentiment": {
            "indices": ["fear_greed", "aaii_bull"],
            "weights": [0.55, 0.45],
            "cat_weight": 0.10,
        },
        "news": {
            "indices": ["news_risk"],
            "weights": [1.00],
            "cat_weight": 0.08,
        },
        "taiwan": {
            "indices": ["tw_foreign_futures", "tw_business_lamp", "sox_ma200"],
            "weights": [0.45, 0.30, 0.25],
            "cat_weight": 0.07,
        },
    }

    # 計算各類別分數
    cat_scores = {}
    for cat_name, cfg in category_weights.items():
        s = sum(raw_scores.get(idx, 40) * w
                for idx, w in zip(cfg["indices"], cfg["weights"]))
        cat_scores[cat_name] = round(s)

    # 計算條件式乘數
    multiplier = compute_condition_multiplier(raw_scores)

    # 加權總分
    base_score = sum(cat_scores[cat] * cfg["cat_weight"]
                     for cat, cfg in category_weights.items())
    final_score = min(100, round(base_score * multiplier))

    # 操作建議
    action = _generate_action(final_score, raw_scores, cat_scores)

    return {
        "score":          final_score,
        "multiplier":     round(multiplier, 2),
        "base_score":     round(base_score),
        "category_scores": cat_scores,
        "raw_scores":     raw_scores,
        "action":         action,
        "level":          _score_level(final_score),
        "computed_at":    datetime.utcnow().isoformat() + "Z",
    }

def _score_level(score: int) -> dict:
    if score <= 25:
        return {"label": "安全",    "color": "green",  "position_advice": "可積極布局或加碼至目標倉位上限"}
    if score <= 45:
        return {"label": "低度警戒","color": "yellow", "position_advice": "維持現有部位，密切觀察指標變化"}
    if score <= 65:
        return {"label": "中度風險","color": "orange", "position_advice": "建議減碼 20–30%，提高現金至 30%"}
    if score <= 80:
        return {"label": "高度警戒","color": "red",    "position_advice": "大幅減碼至股票部位 30% 以下"}
    return         {"label": "極端危險","color": "critical","position_advice": "考慮清倉，持有現金及黃金等避險資產"}

def _generate_action(score: int, raw: dict, cats: dict) -> dict:
    """產生結構化操作建議"""
    top_risks = []

    if raw.get("yield_curve", 0) > 60:
        top_risks.append("殖利率曲線倒掛持續，衰退時鐘滴答作響")
    if raw.get("buffett", 0) > 65:
        top_risks.append(f"Buffett Indicator 過高，整體市場估值偏貴")
    if raw.get("berkshire_cash", 0) > 65:
        top_risks.append("Berkshire現金水位創高，巴菲特找不到便宜貨")
    if raw.get("hy_spread", 0) > 60:
        top_risks.append("高收益債利差擴張，信用市場開始出現壓力")
    if raw.get("tw_foreign_futures", 0) > 65:
        top_risks.append("外資台指期淨空單增加，台股面臨外資對沖壓力")
    if raw.get("news_risk", 0) > 65:
        top_risks.append("地緣政治或政策新聞風險升溫，市場波動率可能擴大")
    if raw.get("cot_net", 0) > 65:
        top_risks.append("CFTC COT 大戶期貨轉向淨空，機構開始對沖")

    # 倉位建議
    if score <= 25:
        us_position   = "70–80%（可積極）"
        tw_position   = "70–80%（可積極）"
        cash_position = "10–20%"
    elif score <= 45:
        us_position   = "60–70%（維持）"
        tw_position   = "60–70%（維持）"
        cash_position = "20–30%"
    elif score <= 65:
        us_position   = "45–60%（減碼）"
        tw_position   = "40–55%（減碼）"
        cash_position = "30–40%"
    elif score <= 80:
        us_position   = "20–35%（大減）"
        tw_position   = "20–35%（大減）"
        cash_position = "50–65%"
    else:
        us_position   = "0–15%（清倉）"
        tw_position   = "0–15%（清倉）"
        cash_position = "75–100%"

    return {
        "top_risks":   top_risks[:4],
        "us_position": us_position,
        "tw_position": tw_position,
        "cash_position": cash_position,
    }

# ─────────────────────────────────────────────
# 主程式
# ─────────────────────────────────────────────
def main():
    data_path  = DATA_DIR / "market_data.json"
    score_path = DATA_DIR / "score.json"

    if not data_path.exists():
        print(f"找不到 {data_path}，請先執行 collect_data.py")
        return

    with open(data_path, encoding="utf-8") as f:
        data = json.load(f)

    result = compute_full_score(data)

    # 儲存評分結果
    with open(score_path, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    print(f"評分完成：{result['score']}/100（{result['level']['label']}）")
    print(f"儲存至 {score_path}")

    # 附加到歷史評分列表
    history_path = DATA_DIR / "score_history.json"
    history = []
    if history_path.exists():
        with open(history_path, encoding="utf-8") as f:
            history = json.load(f)

    history.append({
        "date":  result["computed_at"][:10],
        "score": result["score"],
        "level": result["level"]["label"],
    })
    history = history[-90:]  # 保留最近90天

    with open(history_path, "w", encoding="utf-8") as f:
        json.dump(history, f, ensure_ascii=False)

    return result

if __name__ == "__main__":
    main()
