#!/usr/bin/env python3

import os
import glob
import requests
from pygments import highlight
from pygments.lexers import PythonLexer
from pygments.formatters import ImageFormatter

# === CONFIG (via env / GitHub Secrets) ===
BOT_TOKEN  = os.getenv('TELEGRAM_BOT_TOKEN')
USER_ID    = os.getenv('TELEGRAM_USER_ID')
LOG_LEVELS = os.getenv('LOG_LEVELS', 'ERROR')
LOG_DIR    = os.path.join(os.path.dirname(__file__), '..', 'logs')
# ==========================================

def find_latest_log_file():
    files = sorted(glob.glob(os.path.join(LOG_DIR, '*.log')), reverse=True)
    return files[0] if files else None

def load_filtered_lines(path, levels):
    lvls = [l.strip().upper() for l in levels.split(',')]
    lines = []
    with open(path, 'r', errors='ignore') as f:
        for ln in f:
            if any(lvl in ln.upper() for lvl in lvls):
                lines.append(ln.rstrip())
    return lines

def render_image(text, out_path='log.png'):
    formatter = ImageFormatter(style='monokai', font_name='DejaVu Sans Mono', line_numbers=False)
    img_data = highlight(text, PythonLexer(), formatter)
    with open(out_path, 'wb') as f:
        f.write(img_data)
    return out_path

def send_photo(image_path):
    if not BOT_TOKEN or not USER_ID:
        print('ERROR: BOT_TOKEN or USER_ID not set')
        return
    url = f'https://api.telegram.org/bot{BOT_TOKEN}/sendPhoto'
    with open(image_path, 'rb') as img:
        resp = requests.post(url, data={'chat_id': USER_ID}, files={'photo': img})
    if not resp.ok:
        print(f'ERROR sending photo: {resp.status_code} {resp.text}')

def main():
    log_file = find_latest_log_file()
    if not log_file:
        print('No log files found.')
        return
    lines = load_filtered_lines(log_file, LOG_LEVELS)
    if not lines:
        print('No matching log lines for levels:', LOG_LEVELS)
        return
    text = '\n'.join(lines)
    img = render_image(text)
    send_photo(img)

if __name__ == '__main__':
    main()
