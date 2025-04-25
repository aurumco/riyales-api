#!/usr/bin/env python3
"""
Alert sender for GitHub Actions logs via Telegram.

- Finds newest .log file in logs/
- Filters lines by LOG_LEVELS (e.g. ERROR,WARNING,INFO)
- Renders colored image via Pygments + ImageFormatter
- Sends it as a document (sendDocument) to avoid dimension errors

Required environment variables (as GitHub Secrets):
  TELEGRAM_BOT_TOKEN
  TELEGRAM_USER_ID
  LOG_LEVELS        # comma-separated, e.g. "ERROR,WARNING,INFO"
"""

import os
import glob
import requests
from pygments import highlight
from pygments.lexers import PythonLexer
from pygments.formatters import ImageFormatter

# Configuration (from env/secrets)
BOT_TOKEN  = os.getenv('TELEGRAM_BOT_TOKEN')
USER_ID    = os.getenv('TELEGRAM_USER_ID')
LOG_LEVELS = os.getenv('LOG_LEVELS', 'ERROR')
LOG_DIR    = os.path.join(os.path.dirname(__file__), '..', 'logs')

def find_latest_log_file():
    files = sorted(glob.glob(os.path.join(LOG_DIR, '*.log')), reverse=True)
    return files[0] if files else None

def load_filtered_lines(path, levels):
    lvls = [l.strip().upper() for l in levels.split(',')]
    result = []
    with open(path, 'r', errors='ignore') as f:
        for line in f:
            if any(lvl in line.upper() for lvl in lvls):
                result.append(line.rstrip())
    return result

def render_image(text, out_path='log.png'):
    # Use padding so sendDocument isn't rejected
    formatter = ImageFormatter(
        style='monokai',
        line_numbers=False,
        image_pad=16,
        line_pad=4,
        font_name='DejaVu Sans Mono'
    )
    img_bytes = highlight(text, PythonLexer(), formatter)
    with open(out_path, 'wb') as out:
        out.write(img_bytes)
    return out_path

def send_document(image_path):
    if not BOT_TOKEN or not USER_ID:
        print('ERROR: TELEGRAM_BOT_TOKEN or TELEGRAM_USER_ID not set')
        return
    url = f'https://api.telegram.org/bot{BOT_TOKEN}/sendDocument'
    with open(image_path, 'rb') as img:
        resp = requests.post(
            url,
            data={'chat_id': USER_ID},
            files={'document': img},
            timeout=15
        )
    if not resp.ok:
        print(f'ERROR sending document: {resp.status_code} {resp.text}')

def main():
    log_file = find_latest_log_file()
    if not log_file:
        print('No log files found in logs/')
        return
    lines = load_filtered_lines(log_file, LOG_LEVELS)
    if not lines:
        print(f'No log lines matching levels: {LOG_LEVELS}')
        return
    text = '\n'.join(lines)
    img_path = render_image(text)
    send_document(img_path)

if __name__ == '__main__':
    main()
