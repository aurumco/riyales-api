![Riyales Banner](https://raw.githubusercontent.com/aurumco/riyales-api/main/api/v1/config/Riyales_Dark.png)

# [Riyales](https://ryls.ir/) API - Data Aggregator

This repository contains the automated data aggregation service for the **[Riyales](https://ryls.ir/)** app. It fetches real-time financial data for Iranian markets (Fiat currencies, Gold, TSE/IFB Stocks, Indices, Options, NAVs, Futures, Debt Securities, etc.) and global markets (Cryptocurrencies, Commodities).

The service runs automatically using **GitHub Actions**, collects data from various API endpoints, and stores the latest state in individual files under `api/v1/market/` (JSON) and `api/v2/market/` (Protobuf). Updates occur frequently (every 10 minutes by default, respecting TSE market hours for relevant sources).

## ⚙️ Features

- 🤖 **Automated Execution**: Runs on a schedule via GitHub Actions (cron).
- ⏱️ **Frequent Updates**: Fetches data every 10 minutes for active markets (e.g., Crypto, Gold) and every 20 minutes for TSE/IFB data (during market hours).
- 🇮🇷 **Iran Market Focus**: Comprehensive data coverage for Tehran Stock Exchange (TSE) and Iran Fara Bourse (IFB).
- 🌍 **Global Data**: Includes major Cryptocurrencies and Commodities.
- ⚡ **Efficient Fetching**: Uses an optimized HTTP client for concurrent API requests.
- 💾 **Dual Data Formats**:
  - **v1**: Saves the latest response from each API source into separate JSON files (`api/v1/market/*.json`).
  - **v2**: Uses Protobuf for efficient data serialization and storage (`api/v2/market/*.proto`).
- 📜 **Detailed Logging**: Creates rotating logs in the `logs/` directory.
- 🔄 **Automated Persistence**: Automatically commits updated data (JSONs and Protobufs) and logs back to the repository via GitHub Actions.
- 🔧 **Configurable**: API endpoints, fetch intervals, market hours logic, and other settings are managed within the Python script (`src/main.py`).

## 🧠 Tech Stack

- **Language**: Python 3.13+
- **HTTP Client**: Optimized for concurrent requests
- **Data Formats**: JSON (v1), Protobuf (v2)
- **Scheduling & Execution**: GitHub Actions (cron)
- **Persistence**: Git (via GitHub Actions)

## 📂 Output Structure

- `api/v1/market/` (JSON-based):
  - `all_market_data.json`: Consolidated JSON containing the latest data from all individual endpoint files.
  - `gold.json`, `currency.json`, `cryptocurrency.json`, `commodity.json`, etc.: Latest raw data fetched for each specific endpoint.
- `api/v2/market/` (Protobuf-based):
  - `all_market_data.proto`: Consolidated Protobuf file with the latest data.
  - `gold.proto`, `currency.proto`, `cryptocurrency.proto`, `commodity.proto`, etc.: Latest raw data in Protobuf format.
- `logs/`:
  - `app.log`: General application logs (DEBUG level and above). Rotated frequently.
  - `error.log`: Error logs (ERROR level and above). Rotated less frequently.

## 🧪 Status

✅ Actively developed and used as the core data source for the **[Riyales](https://ryls.ir/)** mobile app.

## 📫 Contact

Made with ❤️ by **Aurum Co.**  
Tehran, Iran 🇮🇷  
Feel free to reach out via [Mail](mailto:mozvfvri@gmail.com) or [Telegram](https://t.me/mozvfvri/).

---
