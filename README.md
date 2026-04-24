"""
RiskRadar — LINE 推播通知模組
使用 LINE Messaging API（免費，每月 200 則）

設定步驟：
  1. 前往 https://developers.line.biz/ 登入你的 LINE 帳號
  2. 建立 Provider（任意名稱）
  3. 建立 Messaging API Channel
  4. 在 Channel 頁面取得 Channel Access Token（長期）
  5. 把這個 Bot 加為好友，傳任意訊息給它
  6. 呼叫 https://api.line.me/v2/bot/profile 取得你的 userId
     或在 Webhook 事件中找到 source.userId
  7. 設定環境變數：LINE_CHANNEL_TOKEN、LINE_USER_ID
"""

import os, json, logging
from pathlib import Path
from datetime import datetime

import requests

log = logging.getLogger("line_notify")

LINE_TOKEN   = os.environ.get("LINE_CHANNEL_TOKEN", "")
LINE_USER_ID = os.environ.get("LINE_USER_ID", "")
DATA_DIR     = Path(__file__).parent.parent / "data"
LAST_SCORE_PATH = DATA_DIR / "last_notified_score_line.json"

LINE_PUSH_URL = "https://api.line.me/v2/bot/message/push"

# ─────────────────────────────────────────────
# 推播門檻（與 Telegram 共用邏輯）
# ─────────────────────────────────────────────
THRESHOLDS = [
    (25,  "cross_up",   "⚠️ 進入低度警戒"),
    (45,  "cross_up",   "🟠 進入中度風險"),
    (65,  "cross_up",   "🔴 進入高度警戒"),
    (80,  "cross_up",   "🆘 進入極端危險"),
    (65,  "cross_down", "📉 風險下降"),
    (45,  "cross_down", "🟡 回到低度警戒"),
    (25,  "cross_down", "✅ 市場回歸安全"),
]

# ─────────────────────────────────────────────
# LINE Flex Message 建構（比純文字更漂亮）
# ─────────────────────────────────────────────
def score_color_hex(score: int) -> str:
    if score <= 25: return "#22c55e"
    if score <= 45: return "#eab308"
    if score <= 65: return "#f97316"
    if score <= 80: return "#ef4444"
    return "#ff4444"

def score_label(score: int) -> str:
    if score <= 25: return "安全"
    if score <= 45: return "低度警戒"
    if score <= 65: return "中度風險"
    if score <= 80: return "高度警戒"
    return "極端危險"

def build_flex_message(score_data: dict, prev_score: int | None, reason: str = "") -> dict:
    """
    建立 LINE Flex Message（視覺卡片格式）
    文件：https://developers.line.biz/en/docs/messaging-api/flex-message-layout/
    """
    score  = score_data["score"]
    level  = score_data.get("level", {})
    cats   = score_data.get("category_scores", {})
    action = score_data.get("action", {})
    risks  = action.get("top_risks", [])
    color  = score_color_hex(score)
    label  = score_label(score)

    # 趨勢
    if prev_score is None:
        trend_text = "初次分析"
    elif score > prev_score + 3:
        trend_text = f"▲ 上升 +{score - prev_score}"
    elif score < prev_score - 3:
        trend_text = f"▼ 下降 -{prev_score - score}"
    else:
        trend_text = "→ 持平"

    # 類別分數列
    cat_map = {
        "valuation": "📈 估值",
        "macro":     "🌍 總經",
        "credit":    "💳 信用",
        "institution":"🏦 法人",
        "sentiment": "😨 情緒",
        "news":      "📰 新聞",
        "taiwan":    "🇹🇼 台股",
    }

    def cat_rows():
        rows = []
        for k, name in cat_map.items():
            v = cats.get(k)
            if v is None:
                continue
            c = score_color_hex(v)
            rows.append({
                "type": "box",
                "layout": "horizontal",
                "spacing": "sm",
                "contents": [
                    {"type": "text", "text": name,
                     "size": "sm", "color": "#8888aa", "flex": 3},
                    {"type": "text", "text": str(v),
                     "size": "sm", "color": c, "flex": 1,
                     "align": "end", "weight": "bold"},
                    {"type": "box", "layout": "vertical", "flex": 4,
                     "justifyContent": "center",
                     "contents": [{
                         "type": "box", "layout": "vertical",
                         "height": "6px",
                         "backgroundColor": "#1e1e28",
                         "cornerRadius": "3px",
                         "contents": [{
                             "type": "box", "layout": "vertical",
                             "height": "6px",
                             "backgroundColor": c,
                             "cornerRadius": "3px",
                             "width": f"{v}%",
                         }]
                     }]
                    }
                ]
            })
        return rows

    def risk_rows():
        if not risks:
            return [{"type": "text", "text": "無重大風險警示",
                     "size": "sm", "color": "#5a5a70"}]
        return [
            {"type": "box", "layout": "horizontal", "spacing": "sm",
             "contents": [
                 {"type": "text", "text": "•", "size": "sm",
                  "color": "#f97316", "flex": 0},
                 {"type": "text", "text": r, "size": "sm",
                  "color": "#9090a8", "wrap": True, "flex": 10},
             ]}
            for r in risks[:3]
        ]

    now_str = datetime.now().strftime("%m/%d %H:%M")

    flex_body = {
        "type": "bubble",
        "styles": {
            "header": {"backgroundColor": "#0a0a0f"},
            "body":   {"backgroundColor": "#111118"},
            "footer": {"backgroundColor": "#0a0a0f"},
        },
        "header": {
            "type": "box", "layout": "vertical",
            "paddingAll": "16px",
            "contents": [
                {"type": "box", "layout": "horizontal",
                 "contents": [
                     {"type": "text", "text": "RiskRadar",
                      "size": "sm", "color": "#7c6fe0", "weight": "bold", "flex": 1},
                     {"type": "text", "text": now_str,
                      "size": "xs", "color": "#5a5a70", "align": "end"},
                 ]},
                {"type": "text", "text": "市場風險指數更新",
                 "size": "xs", "color": "#5a5a70", "margin": "xs"},
            ]
        },
        "body": {
            "type": "box", "layout": "vertical",
            "spacing": "md", "paddingAll": "16px",
            "contents": [
                # 主分數
                {"type": "box", "layout": "horizontal",
                 "alignItems": "flex-end",
                 "contents": [
                     {"type": "text", "text": str(score),
                      "size": "5xl", "color": color, "weight": "bold", "flex": 0},
                     {"type": "box", "layout": "vertical", "flex": 1,
                      "margin": "md",
                      "contents": [
                          {"type": "text", "text": "/100",
                           "size": "sm", "color": "#5a5a70"},
                          {"type": "text", "text": trend_text,
                           "size": "sm", "color": color},
                      ]},
                     {"type": "box", "layout": "vertical", "flex": 0,
                      "contents": [
                          {"type": "text", "text": label,
                           "size": "sm", "color": color, "weight": "bold",
                           "align": "end"},
                      ]},
                 ]},
                # 警示原因
                *([{"type": "text", "text": f"🔔 {reason}",
                    "size": "sm", "color": "#f97316",
                    "margin": "sm"}] if reason else []),
                # 分隔線
                {"type": "separator", "margin": "md",
                 "color": "#2a2a38"},
                # 類別分數
                {"type": "text", "text": "各類別評分",
                 "size": "xs", "color": "#5a5a70", "margin": "md"},
                *cat_rows(),
                # 分隔線
                {"type": "separator", "margin": "md", "color": "#2a2a38"},
                # 主要風險
                {"type": "text", "text": "主要風險",
                 "size": "xs", "color": "#5a5a70", "margin": "md"},
                *risk_rows(),
                # 分隔線
                {"type": "separator", "margin": "md", "color": "#2a2a38"},
                # 倉位建議
                {"type": "text", "text": "倉位建議",
                 "size": "xs", "color": "#5a5a70", "margin": "md"},
                {"type": "box", "layout": "horizontal", "spacing": "sm",
                 "contents": [
                     {"type": "box", "layout": "vertical", "flex": 1,
                      "backgroundColor": "#1e1e28", "cornerRadius": "8px",
                      "paddingAll": "8px",
                      "contents": [
                          {"type": "text", "text": "美股",
                           "size": "xxs", "color": "#5a5a70"},
                          {"type": "text", "text": action.get("us_position", "—"),
                           "size": "xxs", "color": "#e8e8f0", "wrap": True},
                      ]},
                     {"type": "box", "layout": "vertical", "flex": 1,
                      "backgroundColor": "#1e1e28", "cornerRadius": "8px",
                      "paddingAll": "8px",
                      "contents": [
                          {"type": "text", "text": "台股",
                           "size": "xxs", "color": "#5a5a70"},
                          {"type": "text", "text": action.get("tw_position", "—"),
                           "size": "xxs", "color": "#e8e8f0", "wrap": True},
                      ]},
                     {"type": "box", "layout": "vertical", "flex": 1,
                      "backgroundColor": "#1e1e28", "cornerRadius": "8px",
                      "paddingAll": "8px",
                      "contents": [
                          {"type": "text", "text": "現金",
                           "size": "xxs", "color": "#5a5a70"},
                          {"type": "text", "text": action.get("cash_position", "—"),
                           "size": "xxs", "color": "#e8e8f0", "wrap": True},
                      ]},
                 ]},
            ]
        },
        "footer": {
            "type": "box", "layout": "vertical",
            "paddingAll": "12px",
            "contents": [
                {"type": "text",
                 "text": "分數說明：0-25安全 ／ 26-45低警 ／ 46-65中危 ／ 66-80高危 ／ 81+極危",
                 "size": "xxs", "color": "#5a5a70", "wrap": True},
            ]
        }
    }

    return {
        "type": "flex",
        "altText": f"RiskRadar 風險更新：{score}/100（{label}）{' — ' + reason if reason else ''}",
        "contents": flex_body,
    }

# ─────────────────────────────────────────────
# 傳送函式
# ─────────────────────────────────────────────
def send_line_message(messages: list) -> bool:
    if not LINE_TOKEN or not LINE_USER_ID:
        log.warning("LINE 未設定（LINE_CHANNEL_TOKEN 或 LINE_USER_ID 為空），跳過推播")
        return False

    payload = {
        "to":       LINE_USER_ID,
        "messages": messages,
    }
    headers = {
        "Content-Type":  "application/json",
        "Authorization": f"Bearer {LINE_TOKEN}",
    }
    try:
        r = requests.post(LINE_PUSH_URL, json=payload, headers=headers, timeout=15)
        r.raise_for_status()
        log.info(f"LINE 推播成功（userId={LINE_USER_ID}）")
        return True
    except requests.HTTPError as e:
        log.error(f"LINE 推播失敗 HTTP {e.response.status_code}: {e.response.text}")
        return False
    except Exception as e:
        log.error(f"LINE 推播失敗: {e}")
        return False

def send_text_fallback(text: str) -> bool:
    """備用純文字訊息（Flex 失敗時）"""
    return send_line_message([{"type": "text", "text": text}])

# ─────────────────────────────────────────────
# 突發警示（不受門檻限制）
# ─────────────────────────────────────────────
def send_breaking_alert(title: str, detail: str, impact: str):
    msg = f"🚨 突發市場警示\n\n{title}\n\n{detail}\n\n建議動作：{impact}\n\n{datetime.now().strftime('%Y-%m-%d %H:%M')}"
    send_line_message([{"type": "text", "text": msg}])

# ─────────────────────────────────────────────
# 門檻偵測與推播
# ─────────────────────────────────────────────
def check_and_notify_line(score_data: dict, force: bool = False):
    score = score_data["score"]

    prev_score = None
    if LAST_SCORE_PATH.exists():
        with open(LAST_SCORE_PATH) as f:
            prev_score = json.load(f).get("score")

    should_notify = force
    reason = "每日定時推播" if force else ""

    if prev_score is not None and not force:
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
        log.info(f"LINE：分數 {score}（前次 {prev_score}），未觸發門檻")
        return

    log.info(f"LINE 推播：{reason}")

    # 嘗試傳送 Flex Message，失敗則回退純文字
    flex = build_flex_message(score_data, prev_score, reason)
    success = send_line_message([flex])

    if not success:
        # 純文字備用
        label = score_label(score)
        action = score_data.get("action", {})
        text = (f"{'🔔 ' + reason + chr(10) + chr(10) if reason else ''}"
                f"RiskRadar 風險更新\n"
                f"綜合指數：{score}/100（{label}）\n\n"
                f"美股：{action.get('us_position','—')}\n"
                f"台股：{action.get('tw_position','—')}\n"
                f"現金：{action.get('cash_position','—')}\n\n"
                f"{datetime.now().strftime('%Y-%m-%d %H:%M')}")
        success = send_text_fallback(text)

    if success:
        with open(LAST_SCORE_PATH, "w") as f:
            json.dump({"score": score,
                       "notified_at": datetime.utcnow().isoformat()}, f)

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

    check_and_notify_line(score_data, force=force_daily)

if __name__ == "__main__":
    import sys
    force = "--daily" in sys.argv
    main(force_daily=force)
