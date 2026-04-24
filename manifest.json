"""
RiskRadar — Telegram 手機推播通知
設定方式：
  1. 在 Telegram 搜尋 @BotFather，建立新 bot，取得 BOT_TOKEN
  2. 傳訊息給你的 bot，然後訪問：
     https://api.telegram.org/bot{BOT_TOKEN}/getUpdates
     找到你的 chat_id
  3. 設為環境變數：TELEGRAM_BOT_TOKEN、TELEGRAM_CHAT_ID
"""

import os, json, logging
from pathlib import Path
from datetime import datetime

import requests

log = logging.getLogger("notify")

BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
CHAT_ID   = os.environ.get("TELEGRAM_CHAT_ID", "")
DATA_DIR  = Path(__file__).parent.parent / "data"

# 上次通知的評分（避免重複通知）
LAST_SCORE_PATH = DATA_DIR / "last_notified_score.json"

# ─────────────────────────────────────────────
# 通知門檻設定
# ─────────────────────────────────────────────
THRESHOLDS = [
    # (越過此分數時通知, 方向, 訊息等級)
    (25,  "cross_up",   "⚠️ 警戒"),
    (45,  "cross_up",   "🟠 中度風險"),
    (65,  "cross_up",   "🔴 高度警戒"),
    (80,  "cross_up",   "🆘 極端危險"),
    (65,  "cross_down", "🟡 風險下降"),
    (45,  "cross_down", "🟢 回到低度警戒"),
    (25,  "cross_down", "✅ 市場回歸安全"),
]

# ─────────────────────────────────────────────
# Telegram 傳訊函式
# ─────────────────────────────────────────────
def send_message(text: str) -> bool:
    if not BOT_TOKEN or not CHAT_ID:
        log.warning("Telegram 未設定，跳過推播")
        return False
    url  = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    data = {
        "chat_id":    CHAT_ID,
        "text":       text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }
    try:
        r = requests.post(url, json=data, timeout=10)
        r.raise_for_status()
        log.info("Telegram 推播成功")
        return True
    except Exception as e:
        log.error(f"Telegram 推播失敗: {e}")
        return False

def format_score_message(score_data: dict, prev_score: int | None) -> str:
    """產生格式化的推播訊息"""
    score = score_data["score"]
    level = score_data["level"]
    cats  = score_data.get("category_scores", {})
    action= score_data.get("action", {})
    risks = action.get("top_risks", [])

    # 趨勢箭頭
    if prev_score is None:
        arrow = "📊"
    elif score > prev_score + 3:
        arrow = f"⬆️ +{score - prev_score}"
    elif score < prev_score - 3:
        arrow = f"⬇️ -{prev_score - score}"
    else:
        arrow = "↔️"

    # 顏色圓點
    dot = {"green": "🟢", "yellow": "🟡", "orange": "🟠",
           "red": "🔴", "critical": "⛔"}.get(level["color"], "⚪")

    now  = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
    lines = [
        f"<b>RiskRadar 市場風險更新</b>",
        f"",
        f"{dot} <b>綜合風險指數：{score}/100</b>  {arrow}",
        f"狀態：{level['label']}",
        f"",
        f"<b>各類別評分</b>",
        f"📈 估值指標　　{cats.get('valuation', '—')}",
        f"🌍 總體經濟　　{cats.get('macro', '—')}",
        f"💳 信用流動性　{cats.get('credit', '—')}",
        f"🏦 法人動向　　{cats.get('institution', '—')}",
        f"😨 市場情緒　　{cats.get('sentiment', '—')}",
        f"📰 新聞地緣　　{cats.get('news', '—')}",
        f"🇹🇼 台股專區　　{cats.get('taiwan', '—')}",
        f"",
    ]

    if risks:
        lines.append(f"<b>主要風險</b>")
        for r in risks:
            lines.append(f"• {r}")
        lines.append("")

    lines += [
        f"<b>操作建議</b>",
        f"美股倉位：{action.get('us_position', '—')}",
        f"台股倉位：{action.get('tw_position', '—')}",
        f"現金配置：{action.get('cash_position', '—')}",
        f"",
        f"<i>{now}</i>",
    ]

    return "\n".join(lines)

# ─────────────────────────────────────────────
# 推播邏輯（只在門檻被穿越時通知）
# ─────────────────────────────────────────────
def check_and_notify(score_data: dict, force: bool = False):
    """
    force=True：無論如何都發通知（每日定時）
    force=False：只在分數跨越門檻時發通知
    """
    score = score_data["score"]

    # 讀取上次通知分數
    prev_score = None
    if LAST_SCORE_PATH.exists():
        with open(LAST_SCORE_PATH) as f:
            prev_score = json.load(f).get("score")

    should_notify = force
    reason = "每日定時推播" if force else ""

    # 檢查是否穿越任何門檻
    if prev_score is not None and not force:
        for threshold, direction, label in THRESHOLDS:
            if direction == "cross_up"   and prev_score < threshold <= score:
                should_notify = True
                reason = f"風險上升穿越 {threshold} 分 → {label}"
                break
            if direction == "cross_down" and prev_score >= threshold > score:
                should_notify = True
                reason = f"風險下降穿越 {threshold} 分 → {label}"
                break

    if not should_notify:
        log.info(f"分數 {score}（前次 {prev_score}），未觸發門檻，不推播")
        return

    log.info(f"觸發推播：{reason}")
    msg = format_score_message(score_data, prev_score)
    if reason:
        msg = f"🔔 <b>{reason}</b>\n\n" + msg

    success = send_message(msg)

    if success:
        # 更新最後通知分數
        with open(LAST_SCORE_PATH, "w") as f:
            json.dump({"score": score, "notified_at": datetime.utcnow().isoformat()}, f)

# ─────────────────────────────────────────────
# 特殊警示：突發事件
# ─────────────────────────────────────────────
def send_breaking_alert(title: str, detail: str, impact: str):
    """突發事件緊急推播（不受門檻限制）"""
    msg = (
        f"🚨 <b>突發市場警示</b>\n\n"
        f"<b>{title}</b>\n\n"
        f"{detail}\n\n"
        f"<b>建議動作：</b>{impact}\n\n"
        f"<i>{datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}</i>"
    )
    send_message(msg)

# ─────────────────────────────────────────────
# 主程式
# ─────────────────────────────────────────────
def main(force_daily=False):
    score_path = DATA_DIR / "score.json"
    if not score_path.exists():
        log.error("找不到 score.json，請先執行 score_engine.py")
        return

    with open(score_path, encoding="utf-8") as f:
        score_data = json.load(f)

    check_and_notify(score_data, force=force_daily)

if __name__ == "__main__":
    import sys
    force = "--daily" in sys.argv
    main(force_daily=force)
