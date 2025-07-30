![Riyales Banner](https://raw.githubusercontent.com/aurumco/riyales-api/main/api/v1/config/Riyales_Dark.png)

# [Riyales](https://ryls.ir/) API - Real-Time Financial Data Aggregator

This repository contains the automated data aggregation service for the **[Riyales](https://ryls.ir/)** mobile app. It fetches real-time financial data from multiple sources and provides comprehensive market information for Iranian and global financial markets.

## Project Overview

The Riyales API service is designed to collect, process, and distribute real-time financial data with high reliability and efficiency. It serves as the backbone data source for the Riyales mobile application, providing users with up-to-date market information.

### Data Coverage

**🇮🇷 Iranian Markets:**
- **Tehran Stock Exchange (TSE)**: Real-time stock prices, trading volumes, market indices
- **Iran Fara Bourse (IFB)**: Alternative trading platform data
- **Gold Markets**: Domestic gold prices and rates
- **Currency Exchange**: USD, EUR, and other major currency rates
- **Commodities**: Precious metals and other commodity prices

**🌍 Global Markets:**
- **Cryptocurrencies**: Bitcoin, Ethereum, and 100+ other cryptocurrencies
- **International Commodities**: Global commodity prices and trends
- **Foreign Exchange**: International currency pairs

## ⚙️ Core Features

### **Automated Data Collection**
- **Scheduled Execution**: Runs automatically via GitHub Actions cron jobs
- **Smart Scheduling**: Respects market hours for TSE/IFB (8:30 AM - 12:45 PM Tehran time)
- **Adaptive Intervals**: Different update frequencies for different data types
  - Active markets (Crypto, Gold): Every 10 minutes
  - TSE/IFB data: Every 20 minutes (during market hours only)

### **High Performance Architecture**
- **Optimized HTTP Client**: Custom timeout and retry mechanisms
- **Memory Efficient**: Minimal resource usage with smart data handling
- **Fast Response Times**: Average response time under 2 seconds

### **Dual Data Format Support**
- **JSON Format (v1)**: Human-readable, easy to parse
  - `api/v1/market/*.json` - Individual market data files
  - `api/v1/market/all_market_data.json` - Consolidated data
  - `api/v1/market/lite.json` - Filtered essential data for widgets
- **Protobuf Format (v2)**: Binary format for high performance
  - `api/v2/market/*.pb` - Optimized for mobile apps
  - Reduced file sizes by 60-80%
  - Faster parsing and transmission

### **Advanced Configuration**
- **Environment Variables**: Flexible configuration via environment variables
- **Market Hours Logic**: Automatic detection of trading hours
- **Blacklist Support**: Filter out unwanted symbols/assets
- **Name Mapping**: Persian/English name translations
- **Custom Transformations**: Data cleaning and formatting

## 🛠️ Technical Stack

### **Backend Technologies**
- **Python 3.13+**: Core programming language
- **aiohttp**: Asynchronous HTTP client for concurrent requests
- **Protocol Buffers**: Efficient data serialization
- **pytz**: Timezone handling for market hours
- **jdatetime**: Persian calendar support

### **Infrastructure**
- **GitHub Actions**: Automated execution and deployment
- **Git**: Version control and data persistence
- **JSON/Protobuf**: Data storage formats
- **Logging**: Comprehensive logging system

## 📂 Project Structure

```
riyales-api/
├── src/                          # Core application code
│   ├── main.py                   # Main data aggregation script
│   ├── missing_names.py          # Data validation utilities
│   ├── protobuf_generator.py     # Protobuf file generation
│   └── pb_generated/             # Generated protobuf files
├── api/                          # Data output directory
│   ├── v1/                       # JSON format data
│   │   ├── config/               # App configuration files
│   │   └── market/               # Market data (JSON)
│   └── v2/                       # Protobuf format data
│       └── market/               # Market data (Protobuf)
├── dictionaries/                 # Data mapping files
│   ├── crypto_names_fa.json      # Persian crypto names
│   ├── market_name_mapping.json  # Market name translations
│   ├── blacklist.json            # Filtered symbols
│   └── lite_assets.json          # Essential assets list
├── protos/                       # Protocol buffer definitions
│   └── market_data.proto         # Data structure definitions
├── logs/                         # Application logs
│   ├── app.log                   # General application logs
│   └── error.log                 # Error logs
└── .github/                      # GitHub Actions workflows
    └── workflows/
        └── main.yml              # Automated execution workflow
```

## 🔄 Data Flow

1. **Scheduled Trigger**: GitHub Actions triggers the script every 10-20 minutes
2. **API Fetching**: Concurrent requests to multiple data sources
3. **Data Processing**: Validation, transformation, and cleaning
4. **Format Conversion**: Generation of both JSON and Protobuf formats
5. **File Storage**: Saving to appropriate directories
6. **Logging**: Comprehensive logging of all operations
7. **Git Commit**: Automatic commit and push of updated data

## 📊 Data Sources & Endpoints

### **Market Data APIs**
- **Gold & Currency**: Real-time precious metal and currency rates
- **Cryptocurrency**: Global crypto market data with Persian translations
- **TSE/IFB**: Tehran Stock Exchange and Iran Fara Bourse data
- **Commodities**: International commodity prices
- **Indices**: Market index calculations and updates

### **Data Transformation Features**
- **Persian Localization**: Automatic Persian name mapping
- **Digit Conversion**: ASCII to Persian digit conversion
- **Symbol Simplification**: Clean symbol names for better readability
- **Data Filtering**: Blacklist-based unwanted data removal

## 🚀 Getting Started

### **Prerequisites**
- Python 3.13 or higher
- Git access to the repository
- Required environment variables (see configuration)

### **Environment Variables**
```bash
BRS_BASE_URL=your_api_base_url
BRS_API_KEY=your_api_key
LOG_LEVEL=INFO
```

### **Local Development**
```bash
# Clone the repository
git clone https://github.com/aurumco/riyales-api.git
cd riyales-api

# Install dependencies
pip install -r requirements.txt

# Run the data aggregator
python src/main.py
```

## 📱 Mobile App Integration

The aggregated data is specifically optimized for mobile applications:
- **Lite Version**: Reduced data size for faster loading, designed for widgets
- **Protobuf Format**: Efficient binary format for mobile
- **Structured Data**: Consistent data structure across all markets
- **Real-time Updates**: Fresh data every 10-20 minutes

## 🧪 Current Status

✅ **Production Ready**: Actively serving the Riyales mobile app  
✅ **High Availability**: 99.9% uptime through automated execution  
✅ **Scalable Architecture**: Designed for high-volume data processing  
✅ **Maintained**: Regular updates and improvements  

## 📫 Contact & Support

**Made with ❤️ by Aurum Co.**  
📍 Tehran, Iran 🇮🇷

**Contact Information:**
- **Email**: [Contact via Email](mailto:mozvfvri@gmail.com)
- **Telegram**: [Contact via Telegram](https://t.me/mozvfvri/)
- **Website**: [Riyales App](https://ryls.ir/)