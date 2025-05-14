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
# Use current working directory to find logs folder - works in any environment
LOG_DIR = os.path.join(os.getcwd(), 'logs')
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
    """Find the error log file path."""
    error_path = os.path.join(LOG_DIR, ERROR_LOG_FILENAME)
    print(f"• Looking for log file at: {error_path}")
    return error_path if os.path.exists(error_path) else None

def load_and_clean_lines(path, levels):
    """Extract error lines from the log file based on severity levels and git error patterns."""
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
            # Skip empty lines or lines that are just headers from previous clears
            if not ln.strip() or ln.strip().startswith("# Log cleared on") or ln.strip().startswith("# Log forcibly cleared on"):
                continue
                
            upper = ln.upper()
            
            # Match standard log levels or git errors
            is_log_level_match = any(f'|{lvl}' in upper for lvl in lvls)
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
    """Render log text as a nicely formatted image"""
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

def send_telegram_alert(img_path, log_path):
    """Send alert to Telegram with image and log file"""
    if not BOT_TOKEN or not USER_ID:
        print('\033[31m• ERROR: Missing Telegram credentials\033[0m')
        return False

    tehran_tz = pytz.timezone('Asia/Tehran')
    now_utc = datetime.now(pytz.UTC)
    now_tehran = now_utc.astimezone(tehran_tz)
    jnow = jdatetime.datetime.fromgregorian(datetime=now_tehran)
    caption = f"Errors - {jnow.strftime('%Y/%m/%d %H:%M')}"

    # Try sending as media group first
    try:
        url = f'https://api.telegram.org/bot{BOT_TOKEN}/sendDocument'
        with open(img_path, 'rb') as img_file:
            resp = requests.post(
                url,
                data={'chat_id': USER_ID, 'caption': caption},
                files={'document': (os.path.basename(img_path), img_file)},
                timeout=15,
                proxies=PROXIES
            )
            
        if not resp.ok:
            print(f'\033[31m• Failed to send image: {resp.status_code} {resp.text}\033[0m')
            return False
            
        # Send log file separately
        with open(log_path, 'rb') as log_file:
            resp = requests.post(
                url,
                data={'chat_id': USER_ID},
                files={'document': (os.path.basename(log_path), log_file)},
                timeout=15,
                proxies=PROXIES
            )
            
        if resp.ok:
            print('\033[32m• Successfully sent alerts to Telegram\033[0m')
            return True
        else:
            print(f'\033[31m• Failed to send log: {resp.status_code} {resp.text}\033[0m')
            return False
    except Exception as e:
        print(f'\033[31m• Error sending to Telegram: {str(e)}\033[0m')
        return False

def clear_log_file(file_path):
    """Clear the contents of the log file by truncating it."""
    try:
        print(f"• Attempting to clear log file by truncating: {file_path}")
        with open(file_path, 'w') as f:
            pass  # Opening in 'w' mode truncates the file
        print(f'\033[32m• Log file truncated: {file_path}\033[0m')
        return True
    except Exception as e:
        print(f'\033[31m• Failed to truncate log file: {str(e)}\033[0m')
        return False

def main():
    """Main function to check logs, send alerts if needed, and clear logs."""
    # Find the log file
    logfile = find_latest_log_file()
    if not logfile:
        print('\033[33m• No error.log file found\033[0m')
        return

    # Load error lines. If load_and_clean_lines filters out everything (e.g., only headers left),
    # 'lines' will be empty.
    lines = load_and_clean_lines(logfile, LOG_LEVELS)
    
    # If no errors found, check for git errors with extended levels
    if not lines:
        print('\033[34m• No standard error entries found, checking for git errors with extended levels\033[0m')
        # Consider if LOG_LEVELS already includes WARNING or if this is truly extending it.
        extended_levels = LOG_LEVELS # Default to current levels
        if "WARNING" not in LOG_LEVELS.upper():
             extended_levels = f"{LOG_LEVELS},WARNING"
        lines = load_and_clean_lines(logfile, extended_levels)
        
    if not lines:
        print('\033[34m• No actionable error entries found to report\033[0m')
        # Even if no lines to send, we might want to clear the log if it only contained filtered-out content.
        # However, the current problem is re-sending, so only clearing on successful send is key.
        return

    print(f'\033[34m• Found {len(lines)} error lines to report\033[0m')
    
    # Render image of filtered error lines
    text = '\n'.join(lines)
    img_path = render_image(text)

    # Send alert to Telegram
    if send_telegram_alert(img_path, logfile):
        # Clear log file after successful sending
        clear_log_file(logfile)
    else:
        print('\033[31m• Failed to send alert to Telegram, log not cleared\033[0m')

if __name__ == '__main__':
    main()
