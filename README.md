# 🐍 Riyales API - Data Aggregator

This repository contains the automated data aggregation service for the **Riyales** app. It fetches real-time financial data for Iranian markets (Fiat currencies, Gold, TSE/IFB Stocks, Indices, Options, NAVs, Futures, Debt Securities, etc.) and global markets (Cryptocurrencies, Commodities).

The service runs automatically using **GitHub Actions**, collects data from various API endpoints, stores the latest state in individual JSON files, and appends historical data to a **DuckDB** database. Updates occur frequently (every 10–20 minutes, respecting TSE market hours for relevant sources).

---

## ⚙️ Features

- 🤖 **Automated Execution**: Runs on a schedule via GitHub Actions (cron).  
- ⏱️ **Frequent Updates**: Fetches data every 10 minutes for active markets (e.g., Crypto, Gold) and every 20 minutes for TSE/IFB data (during market hours).  
- 🇮🇷 **Iran Market Focus**: Comprehensive data coverage for Tehran Stock Exchange (TSE) and Iran Fara Bourse (IFB).  
- 🌍 **Global Data**: Includes major Cryptocurrencies and Commodities.  
- ⚡ **Asynchronous Fetching**: Uses `aiohttp` for efficient, concurrent API requests.  
- 💾 **Dual Storage**:  
  - Saves the latest response from each API source into separate `data/*.json` files.  
  - Appends timestamped data to `data/historical_data.duckdb` for historical analysis.  
- 📜 **Detailed Logging**: Creates daily rotating logs in the `logs/` directory.  
- 🔄 **Automated Persistence**: Automatically commits updated data (JSON files, DuckDB database) and logs back to the repository via GitHub Actions.  
- 🔧 **Configurable**: API endpoints, fetch intervals, market hours logic, and other settings are managed within the Python script (`src/main.py`).  

---

## 🧠 Tech Stack

- **Language**: Python 3.13+ 🐍  
- **Asynchronous HTTP**: `aiohttp`  
- **Database**: [DuckDB](https://duckdb.org/) 🦆 (for historical data)  
- **Data Format**: JSON  
- **Scheduling & Execution**: GitHub Actions (cron)  
- **Persistence**: Git (via GitHub Actions)  

---

## 📦 Output Structure

- `data/`: Contains the latest fetched data.
  - `gold_currency.json`: Latest Gold, Fiat Currency, some Crypto data.  
  - `cryptocurrency.json`: Latest data for many Cryptocurrencies.  
  - `commodity.json`: Latest Commodity prices (Metals, Energy).  
  - `tse_options.json`: Latest TSE Options data.  
  - `tse_nav.json`: Latest NAV for TSE funds.  
  - `tse_index.json`: Latest TSE Overall Index data.  
  - `ifb_index.json`: Latest IFB Overall Index data.  
  - `selected_indices.json`: Latest data for various selected market indices.  
  - `tse_ifb_symbols.json`: Latest data for all TSE/IFB symbols (Stocks, ETFs, Rights).  
  - `debt_securities.json`: Latest Debt Securities data.  
  - `housing_facilities.json`: Latest Housing Facilities data (Maskan).  
  - `futures.json`: Latest Futures market data.  
- `historical_data.duckdb`: DuckDB database containing historical data for all fetched sources, timestamped in UTC.  
- `historical_data.duckdb.wal`: DuckDB Write-Ahead Log (temporary, ignored by git).  
- `logs/`: Contains daily rotating log files.  
  - `data_fetcher.log`: Main log file for the current day.  
  - `data_fetcher.log.YYYY-MM-DD`: Log files from previous days (kept for 3 days by default).  

---

## 🧪 Status

✅ Actively developed and used as the core data source for the **Riyales** mobile app.

---

## 📫 Contact

Made with ❤️ by **Aurum Co.**  
Tehran, Iran 🇮🇷  
Feel free to reach out via [Mail](mailto:mozvfvri@gmail.com) or [Telegram](https://t.me/mozvfvri/).
