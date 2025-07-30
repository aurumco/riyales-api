#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import sys
from datetime import datetime

GREEN = "\033[32m"
RED = "\033[31m"
YELLOW = "\033[33m"
BLUE = "\033[34m"
RESET = "\033[0m"

CRYPTO_DATA_FILE = "api/v1/market/cryptocurrency.json"
CRYPTO_NAME_MAPPING_FILE = "dictionaries/crypto_names_fa.json"

def print_colored(message, color=BLUE):
    print(f"{color}• {message}{RESET}")

def load_json_file(file_path):
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            return json.load(file)
    except FileNotFoundError:
        print_colored(f"Error: File not found at {file_path}", RED)
        return None
    except json.JSONDecodeError:
        print_colored(f"Error: Invalid JSON format in {file_path}", RED)
        return None
    except Exception as e:
        print_colored(f"Error loading {file_path}: {str(e)}", RED)
        return None

def find_missing_persian_names():
    print_colored(f"Starting search for cryptocurrencies missing Persian names at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    print_colored("Loading cryptocurrency data...")
    crypto_data = load_json_file(CRYPTO_DATA_FILE)
    if not crypto_data:
        return
    
    print_colored("Loading Persian name mapping...")
    name_mapping = load_json_file(CRYPTO_NAME_MAPPING_FILE)
    if not name_mapping:
        return
    
    if not isinstance(crypto_data, list):
        if isinstance(crypto_data, dict) and 'data' in crypto_data:
            crypto_data = crypto_data['data']
        else:
            print_colored("Error: Unexpected cryptocurrency data format", RED)
            return
    
    missing_names = []
    for crypto in crypto_data:
        if 'name' in crypto and crypto['name'] not in name_mapping:
            missing_names.append(crypto['name'])
    
    if missing_names:
        print_colored(f"Found {len(missing_names)} cryptocurrencies missing Persian names:", YELLOW)
        for i, name in enumerate(missing_names, 1):
            print_colored(f"{i}. {name}", YELLOW)
    else:
        print_colored("All cryptocurrencies have Persian name mappings.", GREEN)
    
    return missing_names

if __name__ == "__main__":
    missing_names = find_missing_persian_names()
    
    if missing_names and len(missing_names) > 0:
        print_colored(f"\nTotal missing: {len(missing_names)}", YELLOW)
        sys.exit(1)
    else:
        print_colored("All crypto names have Persian translations!", GREEN)
        sys.exit(0) 