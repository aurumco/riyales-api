# 🐍 Riyales API - Data Aggregator

This repository contains the automated data aggregation service for the **Riyales** app. It fetches real-time financial data for Iranian markets (Fiat currencies, Gold, TSE/IFB Stocks, Indices, Options, NAVs, Futures, Debt Securities, etc.) and global markets (Cryptocurrencies, Commodities).

The service runs automatically using **GitHub Actions**, collects data from various API endpoints, stores the latest state in individual JSON files under `api/v1/market/`, and computes historical aggregates stored in separate JSON files under `api/v1/market/history/`. Updates occur frequently (every 5 minutes by default, respecting TSE market hours for relevant sources).


## ⚙️ Features

- 🤖 **Automated Execution**: Runs on a schedule via GitHub Actions (cron).  
- ⏱️ **Frequent Updates**: Fetches data every 10 minutes for active markets (e.g., Crypto, Gold) and every 20 minutes for TSE/IFB data (during market hours).  
- 🇮🇷 **Iran Market Focus**: Comprehensive data coverage for Tehran Stock Exchange (TSE) and Iran Fara Bourse (IFB).  
- 🌍 **Global Data**: Includes major Cryptocurrencies and Commodities.  
- ⚡ **Asynchronous Fetching**: Uses `aiohttp` for efficient, concurrent API requests.  
- 💾 **JSON-Based Storage**:  
  - Saves the latest response from each API source into separate `api/v1/market/*.json` files.  
  - Computes historical aggregates (e.g., 12h, 24h, 3d, 7d medians) and stores them in `api/v1/market/history/<endpoint>/<interval>.json`.  
- 📜 **Detailed Logging**: Creates rotating logs in the `logs/` directory.  
- 🔄 **Automated Persistence**: Automatically commits updated data (latest JSONs, history JSONs) and logs back to the repository via GitHub Actions.  
- 🔧 **Configurable**: API endpoints, fetch intervals, market hours logic, and other settings are managed within the Python script (`src/main.py`).  


## 🧠 Tech Stack

- **Language**: Python 3.13+ 🐍  
- **Asynchronous HTTP**: `aiohttp`  
- **Data Format**: JSON  
- **Scheduling & Execution**: GitHub Actions (cron)  
- **Persistence**: Git (via GitHub Actions)  


## 📂 Output Structure

- `api/v1/market/`: Contains the latest fetched data.
  - `all_market_data.json`: A consolidated JSON containing the latest data from *all* individual endpoint files below.
  - `gold.json`, `currency.json`, `cryptocurrency.json`, `commodity.json`, etc.: Latest raw data fetched for each specific endpoint.
- `api/v1/market/history/`: Contains historical data and aggregates.
  - `raw_<endpoint>.json`: Raw, timestamped records collected over time for each endpoint (e.g., `raw_crypto.json`). Purged periodically based on `max_interval` in `history_manager.py`.
  - `<endpoint>/`: Subdirectories for each endpoint containing aggregate files.
    - `<interval>.json`: Aggregated data file for a specific interval (e.g., `history/crypto/12h.json`, `history/gold/3d.json`). Contains median prices per symbol for that period.
- `logs/`: Contains rotating log files.
  - `app.log`: General application logs (DEBUG level and above). Rotated frequently.
  - `error.log`: Error logs (ERROR level and above). Rotated less frequently.


## 🧪 Status

✅ Actively developed and used as the core data source for the **Riyales** mobile app.


## 📫 Contact

Made with ❤️ by **Aurum Co.**  
Tehran, Iran 🇮🇷  
Feel free to reach out via [Mail](mailto:mozvfvri@gmail.com) or [Telegram](https://t.me/mozvfvri/).
