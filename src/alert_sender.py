#!/usr/bin/env python3

import os
import jdatetime
import pytz
import requests
import json
from datetime import datetime
from pygments import highlight
from pygments.lexers import TextLexer
from pygments.formatters import ImageFormatter
import re

# Configuration from environment
BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
USER_ID = os.getenv('TELEGRAM_USER_ID')
LOG_LEVELS = os.getenv('LOG_LEVELS', 'ERROR,CRITICAL') # Expecting e.g. "ERROR,WARNING,CRITICAL"
LOG_DIR = os.path.join(os.path.dirname(__file__), '..', 'logs')
ERROR_LOG_FILENAME = 'error.log'

# Proxy configuration
USE_PROXY = False
PROXY_HOST = '127.0.0.1'
PROXY_PORT = 10808
PROXIES = {
    'https': f'https://{PROXY_HOST}:{PROXY_PORT}',
    'http': f'http://{PROXY_HOST}:{PROXY_PORT}'
} if USE_PROXY else None

def find_latest_log_file():
    error_path = os.path.join(LOG_DIR, ERROR_LOG_FILENAME)
    return error_path if os.path.exists(error_path) else None

def load_and_clean_lines(path, levels):
    lvls = [lvl.strip().upper() for lvl in levels.split(',')]
    out = []
    
    # Git error patterns to detect
    git_error_patterns = [
        r"<<<<<+",         # Merge conflict marker start
        r">>>>>+",         # Merge conflict marker end
        r"=====+",         # Merge conflict separator
        r"CONFLICT",       # Conflict message
        r"error:",         # Git error message lowercase
        r"ERROR:",         # Git error message uppercase
        r"fatal:",         # Git fatal error lowercase
        r"FATAL:",         # Git fatal error uppercase
        r"Failed to",      # Common error message
        r"cannot|can't",   # Common error terms
        r"Permission denied",  # Access issues
        r"could not",      # Common error phrase
        r"rejected",       # Git push/pull rejection
        r"refusing to",    # Git refusal messages
        r"Aborting"        # Operation abort
    ]
    
    # Create a combined pattern to match any git error
    git_pattern = re.compile('|'.join(git_error_patterns), re.IGNORECASE)
    
    with open(path, 'r', errors='ignore') as f:
        for ln in f:
            upper = ln.upper()
            
            # Match standard log levels
            is_log_level_match = any(f'|{lvl}' in upper for lvl in lvls)
            
            # Match git errors using regex
            is_git_error = git_pattern.search(ln) is not None
            
            if is_log_level_match or is_git_error:
                # Normalize pipe-delimited logs or raw lines
                if '|' in ln:
                    parts = [p.strip() for p in ln.split('|')]
                    out.append('|'.join(parts).rstrip())
                else:
                    out.append(ln.strip())
    
    return out

def determine_highest_severity(lines):
    """Determine the highest severity level in the log lines"""
    if not lines:
        return None
        
    levels = ["CRITICAL", "ERROR", "WARNING", "INFO"]
    
    for level in levels:
        for line in lines:
            if f"|{level}" in line.upper():
                return level
    
    return None

def render_image(text, out='log.png'):
    overrides = {
        'Background': '#1a1a1a',
        'Text': '#f8f8f8',
        'Error': '#ff5555',
        'Name': '#50fa7b',
        'Literal': '#f1fa8c',
        'String': '#8be9fd',
        'Keyword': '#ff79c6',
        'Comment': '#6272a4',
    }
    fmt = ImageFormatter(
        style='monokai',
        line_numbers=False,
        image_pad=24,
        line_pad=6,
        font_name='DejaVu Sans Mono',
        font_size=18,
        style_overrides=overrides,
    )
    img = highlight(text, TextLexer(), fmt)
    with open(out, 'wb') as f:
        f.write(img)
    return out

def create_severity_keyboard(highest_severity=None):
    """Create an inline keyboard with severity levels"""
    buttons = []

    severity_buttons = [
        {"text": "🔴 Error", "callback_data": "severity_error"},
        {"text": "🟡 Warning", "callback_data": "severity_warning"},
        {"text": "🟠 Critical", "callback_data": "severity_critical"},
        {"text": "🔵 Info", "callback_data": "severity_info"}
    ]
    
    buttons.append(severity_buttons)
    
    return {"inline_keyboard": buttons}

def send_document(path, lines):
    if not BOT_TOKEN or not USER_ID:
        print('\033[31m• ERROR: Missing Telegram credentials\033[0m')
        return False
        
    tehran_tz = pytz.timezone('Asia/Tehran')
    now_utc = datetime.now(pytz.UTC)
    now_tehran = now_utc.astimezone(tehran_tz)
    jnow = jdatetime.datetime.fromgregorian(datetime=now_tehran)
    
    caption = f"Errors - {jnow.strftime('%Y/%m/%d %H:%M')}"
    
    highest_severity = determine_highest_severity(lines)
    
    reply_markup = create_severity_keyboard(highest_severity)
    
    url = f'https://api.telegram.org/bot{BOT_TOKEN}/sendDocument'
    try:
        with open(path, 'rb') as doc:
            resp = requests.post(
                url,
                data={
                    'chat_id': USER_ID, 
                    'caption': caption,
                    'reply_markup': json.dumps(reply_markup)
                },
                files={'document': (os.path.basename(path), doc)},
                timeout=15,
                proxies=PROXIES
            )
        if not resp.ok:
            print(f'\033[31m• ERROR sending document: {resp.status_code} {resp.text}\033[0m')
            return False
        return True
    except Exception as e:
        print(f'\033[31m• ERROR sending document: {str(e)}\033[0m')
        return False

def send_media_group(img_path: str, log_path: str, reply_markup: dict) -> bool:
    """Sends image and log file together as a Telegram media group."""
    if not BOT_TOKEN or not USER_ID:
        print('\033[31m• ERROR: Missing Telegram credentials\033[0m')
        return False

    tehran_tz = pytz.timezone('Asia/Tehran')
    now_utc = datetime.now(pytz.UTC)
    now_tehran = now_utc.astimezone(tehran_tz)
    jnow = jdatetime.datetime.fromgregorian(datetime=now_tehran)
    caption = f"Errors - {jnow.strftime('%Y/%m/%d %H:%M')}"

    # First try sending as media group
    try:
        url = f'https://api.telegram.org/bot{BOT_TOKEN}/sendMediaGroup'
        media = [
            {'type': 'document', 'media': 'attach://img', 'caption': caption},
            {'type': 'document', 'media': 'attach://log'}
        ]
        data = {
            'chat_id': USER_ID,
            'media': json.dumps(media)
            # Reply markup not supported in media groups by Telegram API
        }
        files = {
            'img': (os.path.basename(img_path), open(img_path, 'rb')),
            'log': (os.path.basename(log_path), open(log_path, 'rb'))
        }

        resp = requests.post(url, data=data, files=files, timeout=15, proxies=PROXIES)
        
        # Make sure to close the file handles
        for f in files.values():
            f[1].close()
            
        if resp.ok:
            print('\033[32m• Successfully sent media group to Telegram\033[0m')
            return True
        else:
            print(f'\033[31m• Failed to send media group: {resp.status_code} {resp.text}\033[0m')
            # Fall through to send individual documents
    except Exception as e:
        print(f'\033[31m• Error sending media group: {str(e)}\033[0m')
        # Fall through to send individual documents
    
    # If sending media group failed, try sending documents individually
    print('\033[33m• Falling back to individual document sending\033[0m')
    success = send_document(img_path, []) and send_document(log_path, [])
    return success

def main():
    # Use the fixed error log
    logfile = find_latest_log_file()
    if not logfile:
        print('\033[33m• No error.log file found\033[0m')
        return

    # Load error lines according to configured levels
    lines = load_and_clean_lines(logfile, LOG_LEVELS)
    
    # If no errors found in log levels, do a second pass for git errors only
    if not lines:
        print('\033[34m• No standard error log entries found, checking for git errors\033[0m')
        # Force include WARNING level to catch more potential issues
        extended_levels = f"{LOG_LEVELS},WARNING"
        lines = load_and_clean_lines(logfile, extended_levels)
        
    if not lines:
        print('\033[34m• No matching log entries found\033[0m')
        return

    print(f'\033[34m• Found {len(lines)} error lines to report\033[0m')
    
    # Render image of filtered error lines
    text = '\n'.join(lines)
    img_path = render_image(text)

    # Send both image and log file as a media group
    reply_markup = create_severity_keyboard(determine_highest_severity(lines))
    if send_media_group(img_path, logfile, reply_markup):
        print('\033[32m✓ Successfully sent error alert (image + log) to Telegram\033[0m')
        # Clear the sent logs to avoid duplicate alerts
        try:
            open(logfile, 'w').close()
            print('\033[34m• Cleared error.log after sending alert\033[0m')
        except Exception as e:
            print(f'\033[31m• ERROR clearing log file: {e}\033[0m')
    else:
        print('\033[31m• Failed to send error alert to Telegram\033[0m')

if __name__ == '__main__':
    main()
