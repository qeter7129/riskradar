"""
RiskRadar — Telegram 推播通知
執行：python notify.py          （只在門檻觸發時推播）
執行：python notify.py --daily  （強制推播，每天早上用）
"""

import os, json, sys, logging
from pathlib import Path
from datetime import datetime

import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
log = logging.getLogger("telegram")

BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
CHAT_ID   = os.environ.get("TELEGRAM_CHAT_ID", "")

# 資料檔路徑（根目錄 data/ 資料夾）
DATA_DIR        = Path(__file__).parent / "data"
SCORE_PATH      = DATA_DIR / "score.json"
LAST_NOTIF_PATH = DATA_DIR / "last_notified.json"

# 推播門檻：分數跨越這些數字時才通知
THRESHOLDS = [
    (25, "cross_up",   "⚠️ 進入低度警戒區"),
    (45, "cross_up",   "🟠 進入中度風險區"),
    (65, "cross_up",   "🔴 進入高度警戒區"),
    (80, "cross_up",   "🆘 極端危險！"),
    (65, "cross_down", "📉 風險下降，離開高危區"),
    (45, "cross_down", "🟡 回到低度警戒"),
    (25, "cross_down", "✅ 市場回歸安全區"),
]

def score_color(s):
    if s <= 25: return "🟢"
    if s <= 45: return "🟡"
    if s <= 65: return "🟠"
    if s <= 80: return "🔴"
    return "⛔"

def score_label(s):
    if s <= 25: return "安全"
    if s <= 45: return "低度警戒"
    if s <= 65: return "中度風險"
    if s <= 80: return "高度警戒"
    return "極端危險"

def send(text: str) -> bool:
    if not BOT_TOKEN or not CHAT_ID:
        log.warning("未設定 TELEGRAM_BOT_TOKEN 或 TELEGRAM_CHAT_ID")
        return False
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    try:
        r = requests.post(url, json={
            "chat_id": CHAT_ID,
            "text": text,
            "parse_mode": "HTML",
        }, timeout=15)
        r.raise_for_status()
        log.info("Telegram 推播成功")
        return True
    except Exception as e:
        log.error(f"Telegram 推播失敗: {e}")
        return False

def build_message(data: dict, prev: int | None, reason: str) -> str:
    s      = data["score"]
    cats   = data.get("category_scores", {})
    action = data.get("action", {})
    risks  = action.get("top_risks", [])
    now    = datetime.now().strftime("%m/%d %H:%M")

    # 趨勢
    if prev is None:       trend = "📊 首次分析"
    elif s > prev + 3:     trend = f"⬆️ 上升 +{s - prev}"
    elif s < prev - 3:     trend = f"⬇️ 下降 -{prev - s}"
    else:                  trend = "↔️ 持平"

    lines = []
    if reason:
        lines.append(f"🔔 <b>{reason}</b>\n")

    lines += [
        f"<b>RiskRadar 市場風險更新</b>  {now}",
        "",
        f"{score_color(s)} <b>綜合風險指數：{s}/100</b>  {trend}",
        f"狀態：{score_label(s)}",
        "",
        "📊 <b>各類別評分</b>",
        f"  📈 估值指標　　{cats.get('valuation', '—')}",
        f"  🌍 總體經濟　　{cats.get('macro', '—')}",
        f"  💳 信用流動性　{cats.get('credit', '—')}",
        f"  🏦 法人動向　　{cats.get('institution', '—')}",
        f"  😨 市場情緒　　{cats.get('sentiment', '—')}",
        f"  📰 新聞地緣　　{cats.get('news', '—')}",
        f"  🇹🇼 台股專區　　{cats.get('taiwan', '—')}",
        "",
    ]

    if risks:
        lines.append("⚠️ <b>主要風險</b>")
        for r in risks[:3]:
            lines.append(f"  • {r}")
        lines.append("")

    lines += [
        "💼 <b>倉位建議</b>",
        f"  美股：{action.get('us_position', '—')}",
        f"  台股：{action.get('tw_position', '—')}",
        f"  現金：{action.get('cash_position', '—')}",
        "",
        "<i>評分說明：0-25安全 / 26-45低警 / 46-65中危 / 66-80高危 / 81+極危</i>",
    ]

    return "\n".join(lines)

def main(force_daily=False):
    if not SCORE_PATH.exists():
        log.error(f"找不到 {SCORE_PATH}，請先執行 score_engine.py")
        return

    with open(SCORE_PATH, encoding="utf-8") as f:
        data = json.load(f)

    score = data["score"]

    # 讀上次通知分數
    prev_score = None
    if LAST_NOTIF_PATH.exists():
        with open(LAST_NOTIF_PATH) as f:
            prev_score = json.load(f).get("score")

    # 判斷是否需要推播
    should_notify = force_daily
    reason = "每日定時更新" if force_daily else ""

    if prev_score is not None and not force_daily:
        for threshold, direction, label in THRESHOLDS:
            if direction == "cross_up"   and prev_score < threshold <= score:
                should_notify = True
                reason = f"{label}（{prev_score} → {score}）"
                break
            if direction == "cross_down" and prev_score >= threshold > score:
                should_notify = True
                reason = f"{label}（{prev_score} → {score}）"
                break

    if not should_notify:
        log.info(f"分數 {score}（前次 {prev_score}），未觸發門檻，跳過推播")
        return

    msg = build_message(data, prev_score, reason)
    success = send(msg)

    if success:
        DATA_DIR.mkdir(exist_ok=True)
        with open(LAST_NOTIF_PATH, "w") as f:
            json.dump({"score": score, "at": datetime.utcnow().isoformat()}, f)

if __name__ == "__main__":
    force = "--daily" in sys.argv
    main(force_daily=force)
