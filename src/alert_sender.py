#!/usr/bin/env python3
"""
Alert sender for GitHub Actions logs via Telegram.

- Finds the newest .log in logs/
- Filters only ERROR-level lines
- Cleans up extra spaces (e.g. "|INFO   |" → "|INFO|")
- Converts Gregorian timestamp to Jalali for the caption
- Renders a syntax-highlighted image with custom colors & dark background
- Sends it via Telegram sendDocument

Required env vars (GitHub Secrets):
  TELEGRAM_BOT_TOKEN
  TELEGRAM_USER_ID
  LOG_LEVELS (e.g. "ERROR")
"""

import os
import glob
import jdatetime
import requests
from pygments import highlight
from pygments.lexers import TextLexer
from pygments.formatters import ImageFormatter

# === Config from env ===
BOT_TOKEN  = os.getenv('TELEGRAM_BOT_TOKEN')
USER_ID    = os.getenv('TELEGRAM_USER_ID')
LOG_LEVELS = os.getenv('LOG_LEVELS', 'ERROR')
LOG_DIR    = os.path.join(os.path.dirname(__file__), '..', 'logs')
# =======================

def find_latest_log_file():
    files = sorted(glob.glob(os.path.join(LOG_DIR, '*.log')), reverse=True)
    return files[0] if files else None

def load_and_clean_lines(path, level):
    out = []
    with open(path, 'r', errors='ignore') as f:
        for ln in f:
            if f'|{level}' in ln.upper():
                # remove extra spaces around pipes
                clean = '|'.join(part.strip() for part in ln.split('|'))
                out.append(clean)
    return out

def render_image(text, out='log.png'):
    # custom dark background + bright colors
    style_overrides = {
        'Background':      '#0d0f11',
        'Text':            '#e0e0e0',
        'Error':           '#ff5555',
        'Keyword':         '#f1fa8c',
        'Name':            '#50fa7b',
        'Literal.String':  '#8be9fd',
    }
    formatter = ImageFormatter(
        style='monokai',
        line_numbers=False,
        image_pad=16,
        line_pad=4,
        font_name='DejaVu Sans Mono',
        font_size=20,
        style_overrides=style_overrides,  # apply our custom palette
    )
    img_data = highlight(text, TextLexer(), formatter)
    with open(out, 'wb') as img:
        img.write(img_data)
    return out

def send_document(path):
    if not BOT_TOKEN or not USER_ID:
        print('ERROR: Missing Telegram credentials')
        return
    # Caption: Jalali date + time
    now = jdatetime.datetime.now()
    caption = now.strftime('خطاها - %Y/%m/%d %H:%M:%S')
    url = f'https://api.telegram.org/bot{BOT_TOKEN}/sendDocument'
    with open(path, 'rb') as doc:
        resp = requests.post(
            url,
            data={'chat_id': USER_ID, 'caption': caption},
            files={'document': doc},
            timeout=15
        )
    if not resp.ok:
        print(f'ERROR sending document: {resp.status_code} {resp.text}')

def main():
    logfile = find_latest_log_file()
    if not logfile:
        print('No .log files found')
        return
    lines = load_and_clean_lines(logfile, level='ERROR')
    if not lines:
        print('No ERROR lines to send')
        return
    text = '\n'.join(lines)
    img = render_image(text)
    send_document(img)

if __name__ == '__main__':
    main()
