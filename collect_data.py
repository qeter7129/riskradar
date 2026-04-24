name: RiskRadar 數據更新

on:
  schedule:
    # 每天台灣時間 8:00、12:00、18:00、22:00 執行（UTC+8）
    - cron: "0 0,4,10,14 * * *"
  workflow_dispatch:  # 允許手動觸發

jobs:
  collect-and-score:
    runs-on: ubuntu-latest

    steps:
      - name: Checkout repo
        uses: actions/checkout@v4
        with:
          fetch-depth: 0
          token: ${{ secrets.GITHUB_TOKEN }}

      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: "3.11"
          cache: "pip"

      - name: Install dependencies
        run: |
          pip install -r backend/requirements.txt

      - name: 蒐集市場數據
        env:
          FRED_API_KEY: ${{ secrets.FRED_API_KEY }}
        run: |
          cd backend
          python collect_data.py

      - name: 計算風險評分
        run: |
          cd backend
          python score_engine.py

      - name: 推播 — LINE
        env:
          LINE_CHANNEL_TOKEN: ${{ secrets.LINE_CHANNEL_TOKEN }}
          LINE_USER_ID:       ${{ secrets.LINE_USER_ID }}
        run: |
          cd backend
          HOUR=$(date -u +%H)
          if [ "$HOUR" = "00" ]; then
            python notify_line.py --daily
          else
            python notify_line.py
          fi

      - name: 推播 — Telegram（選填）
        env:
          TELEGRAM_BOT_TOKEN: ${{ secrets.TELEGRAM_BOT_TOKEN }}
          TELEGRAM_CHAT_ID:   ${{ secrets.TELEGRAM_CHAT_ID }}
        run: |
          cd backend
          HOUR=$(date -u +%H)
          if [ "$HOUR" = "00" ]; then
            python notify.py --daily
          else
            python notify.py
          fi
        continue-on-error: true

      - name: 更新 GitHub Pages 數據
        run: |
          git config user.name  "RiskRadar Bot"
          git config user.email "bot@riskradar.local"
          git add data/
          git diff --staged --quiet || git commit -m "chore: 更新市場數據 $(date -u '+%Y-%m-%d %H:%M UTC')"
          git push
