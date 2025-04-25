#!/usr/bin/env python3
"""
Alert sender for GitHub Actions logs via Telegram.

Configuration:
- BOT_TOKEN:      Telegram bot token (secret: TELEGRAM_BOT_TOKEN)
- USER_ID:        Telegram user ID (secret: TELEGRAM_USER_ID)
- LOG_LEVELS:     Comma-separated log levels to send (e.g., "ERROR,WARNING")

Example log levels:
- DEBUG
- INFO
- WARNING
- ERROR
- CRITICAL
"""

import os
import glob
import argparse
import requests

# === CONFIG ===
BOT_TOKEN   = os.getenv('TELEGRAM_BOT_TOKEN')
USER_ID     = os.getenv('TELEGRAM_USER_ID')
LOG_LEVELS  = os.getenv('LOG_LEVELS', 'ERROR')
LOG_DIR     = os.path.join(os.path.dirname(__file__), '..', 'logs')
# ==============

def find_latest_log_file():
    log_files = sorted(glob.glob(os.path.join(LOG_DIR, '*.log')), reverse=True)
    return log_files[0] if log_files else None

def load_messages(log_file, levels):
    levels = [lvl.strip().upper() for lvl in levels.split(',')]
    msgs = []
    with open(log_file, 'r', errors='ignore') as f:
        for line in f:
            for lvl in levels:
                if lvl in line.upper():
                    msgs.append(line.rstrip())
                    break
    return msgs

def send_telegram(messages):
    if not BOT_TOKEN or not USER_ID:
        print('ERROR: BOT_TOKEN or USER_ID not set')
        return
    if not messages:
        print('No log messages to send')
        return
    text = '\n'.join(messages)
    url = f'https://api.telegram.org/bot{BOT_TOKEN}/sendMessage'
    payload = {
        'chat_id': USER_ID,
        'text': text,
        'disable_web_page_preview': True
    }
    resp = requests.post(url, data=payload, timeout=10)
    if not resp.ok:
        print(f'ERROR: Telegram API response: {resp.status_code} {resp.text}')

def main():
    log_file = find_latest_log_file()
    if not log_file:
        print('No log files found.')
        return
    msgs = load_messages(log_file, LOG_LEVELS)
    send_telegram(msgs)

if __name__ == '__main__':
    main()
