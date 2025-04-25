#!/usr/bin/env python3

import os
import glob
import jdatetime
import requests
from pygments import highlight
from pygments.lexers import TextLexer
from pygments.formatters import ImageFormatter

# Configuration from environment
BOT_TOKEN  = os.getenv('TELEGRAM_BOT_TOKEN')
USER_ID    = os.getenv('TELEGRAM_USER_ID')
# Expecting e.g. "ERROR,CRITICAL"
LOG_LEVELS = os.getenv('LOG_LEVELS', 'ERROR,CRITICAL')
LOG_DIR    = os.path.join(os.path.dirname(__file__), '..', 'logs')

def find_latest_log_file():
    files = sorted(glob.glob(os.path.join(LOG_DIR, '*.log')), reverse=True)
    return files[0] if files else None

def load_and_clean_lines(path, levels):
    lvls = [lvl.strip().upper() for lvl in levels.split(',')]
    out = []
    with open(path, 'r', errors='ignore') as f:
        for ln in f:
            upper = ln.upper()
            if any(f'|{lvl}' in upper for lvl in lvls):
                # collapse spaces around pipes
                parts = [p.strip() for p in ln.split('|')]
                out.append('|'.join(parts).rstrip())
    return out

def render_image(text, out='log.png'):
    # custom dark background + vivid colors
    overrides = {
        'Background':      '#0d0f11',
        'Text':            '#e0e0e0',
        'Error':           '#ff5555',
        'Name':            '#50fa7b',
        'Literal':         '#f1fa8c',
        'String':          '#8be9fd',
    }
    fmt = ImageFormatter(
        style='monokai',
        line_numbers=False,
        image_pad=16,
        line_pad=4,
        font_name='DejaVu Sans Mono',
        font_size=20,
        style_overrides=overrides,
    )
    img = highlight(text, TextLexer(), fmt)
    with open(out, 'wb') as f:
        f.write(img)
    return out

def send_document(path):
    if not BOT_TOKEN or not USER_ID:
        print('ERROR: Missing Telegram credentials')
        return
    # Caption with Jalali datetime
    now = jdatetime.datetime.now()
    caption = now.strftime('خطاها - %Y/%m/%d %H:%M:%S')
    url = f'https://api.telegram.org/bot{BOT_TOKEN}/sendDocument'
    with open(path, 'rb') as doc:
        resp = requests.post(
            url,
            data={'chat_id': USER_ID, 'caption': caption},
            files={'document': (os.path.basename(path), doc)},
            timeout=15
        )
    if not resp.ok:
        print(f'ERROR sending document: {resp.status_code} {resp.text}')

def main():
    logfile = find_latest_log_file()
    if not logfile:
        print('No .log files found')
        return
    lines = load_and_clean_lines(logfile, LOG_LEVELS)
    if not lines:
        print('No ERROR | CRITICAL lines to send')
        return
    text = '\n'.join(lines)
    img = render_image(text)
    send_document(img)

if __name__ == '__main__':
    main()
