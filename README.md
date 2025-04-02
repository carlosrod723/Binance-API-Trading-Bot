# API Trading Bot

A high-performance, async trading bot built with Sanic, supporting automated trading on Binance and Coinbase. The bot uses predefined strategies (e.g., MACD) to analyze market data, execute trades, and manage positions, with extensibility for adding new exchanges and strategies.

## Features

- **Exchanges**: Binance (live/testnet) and Coinbase (live/sandbox) support.
- **Strategies**: Modular strategy system with a sample MACD implementation.
- **API Endpoints**: Manage balances, orders, positions, strategies, and bot lifecycle.
- **Async Architecture**: Built on Sanic for non-blocking performance.
- **Dockerized**: Containerized deployment with TA-Lib support.
- **Testing**: Comprehensive pytest suite for utilities and strategies.

## Prerequisites

- **Python**: 3.11+
- **Docker**: For containerized deployment.
- **API Keys**:
  - Binance: Live (`BINANCE_API_KEY`, `BINANCE_API_SECRET`) and Testnet (`BINANCE_TESTNET_API_KEY`, `BINANCE_TESTNET_API_SECRET`).
  - Coinbase: Live (`COINBASE_API_KEY`, `COINBASE_API_SECRET`, `COINBASE_PASSPHRASE`) and Sandbox (`COINBASE_SANDBOX_API_KEY`, `COINBASE_SANDBOX_API_SECRET`, `COINBASE_SANDBOX_PASSPHRASE`).

## Setup

### Local Development

1. **Clone the Repository**:

```bash
   git clone https://github.com/yourusername/api-trading-bot.git
   cd api-trading-bot
```

2. Create a Virtual Environment:

```bash
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
```

3. Install Dependencies:

```bash
pip install -r requirements.txt
```

- Ensure libta-lib0 is installed for TA-Lib:

```bash
sudo apt-get update && sudo apt-get install -y libta-lib0  # Debian/Ubuntu
```

4. Configure Environment Variables:

- Create a .env file in the root directory:

BINANCE_API_KEY=your_live_key
BINANCE_API_SECRET=your_live_secret
BINANCE_TESTNET_API_KEY=your_testnet_key
BINANCE_TESTNET_API_SECRET=your_testnet_secret
COINBASE_API_KEY=your_live_key
COINBASE_API_SECRET=your_live_secret
COINBASE_PASSPHRASE=your_live_passphrase
COINBASE_SANDBOX_API_KEY=your_sandbox_key
COINBASE_SANDBOX_API_SECRET=your_sandbox_secret
COINBASE_SANDBOX_PASSPHRASE=your_sandbox_passphrase
DEBUG=false
WORKERS=4
PORT=8000

5. Run the Backend:

```bash
python app/main.py
```

- Access the API at http://localhost:8000/

6. Run the Frontend (optional):

```bash
cd binance-trading-dashboard
npm install
npm start
```

- Access the dashboard at http://localhost:3000/

## Docker Deployment

### Backend Only

1. Build the Backend Docker Image:

```bash
docker build -t trading-bot-backend .
```

2. Run the Backend Container:

```bash
docker run -p 8000:8000 --env-file .env trading-bot-backend
```

- Optionally override env vars:

```bash
docker run -p 8000:8000 -e PORT=8080 --env-file .env trading-bot-backend
```

### Full Stack Deployment with Docker Compose

For deploying both the backend and frontend together:

1. Ensure you have a `.env` file in the root directory with your credentials:

```
BINANCE_API_KEY=your_api_key_here
BINANCE_API_SECRET=your_api_secret_here
```

2. Build and start both containers using Docker Compose:

```bash
docker-compose up -d
```

3. Access the application:
   - Frontend Dashboard: http://localhost
   - Backend API: http://localhost:8000

4. Stop the containers:

```bash
docker-compose down
```

## Usage

### API Endpoints

Balance:

- GET /api/v1/balance/overall?exchange=binance: Total USD balance.
- GET /api/v1/balance/coins?exchange=coinbase: Coin-specific balances.

Orders:

- POST /api/v1/order/buy: Create a buy order.

```json
{
  "coinPair": "BTCUSDT",
  "quantity": 0.001,
  "price": 50000,
  "exchange": "binance"
}
```

- POST /api/v1/order/sell: Create a sell order.
- POST /api/v1/order/exit: Exit a position.

Positions:

- GET /api/v1/positions/open?exchange=binance: Open positions.
- GET /api/v1/positions/closed?exchange=coinbase: Closed positions.

Strategy:

- POST /api/v1/strategy/: Select a strategy.

````json
{"strategyName": "macd_strategy"}

* POST /api/v1/strategy/parameters: Set strategy params.
* GET /api/v1/strategy/list: List available strategies.
* GET /api/v1/strategy/indicators: List TA-Lib indicators.

Bot Control:
* POST /api/v1/bot/start: Start the bot.
```json
{"strategyName": "macd_strategy", "coinPair": "BTCUSDT", "exchange": "binance"}
````

- POST /api/v1/bot/stop: Stop the bot.

```json
{ "exchange": "binance" }
```

- Headers: All endpoints require X-MBX-APIKEY with a valid API key.

## Example Request

```bash
curl -X POST -H "X-MBX-APIKEY: your_testnet_key" -d '{"coinPair":"BTCUSDT","quantity":0.001,"price":50000,"exchange":"binance"}' http://localhost:8000/api/v1/order/buy
```

## Testing

Run the test suite with pytest:

```bash
pytest tests/ -v
```

- Requirements: pytest, pytest-asyncio (in requirements.txt).
  Tests:
  test_binance_utils.py: Binance Testnet functionality.
  test_coinbase_utils.py: Coinbase Sandbox functionality.
  test_strategies.py: MACD strategy logic.
  test_connection.py: Basic Binance connectivity.
- Ensure .env is configured with test credentials.

## User Interface Showcase

The trading dashboard features a modern, responsive UI designed for optimal trading experience.

### Login Page
![Login Page](images/LoginPage.png)
The secure login page allows users to connect using their Binance API credentials, with an option to toggle between live trading and testnet environments.

### Dashboard Overview
![Dashboard](images/DashboardPage.png)
The main dashboard provides a comprehensive overview of account balance, active positions, and recent trading activity in a clean, organized layout.

### Market Data Analysis
![Market Page](images/MarketPage.png)
The market page features advanced charting capabilities, real-time price updates, and market depth visualization to help traders make informed decisions.

### Position Management
![Positions Page](images/PositionsPage.png)
Keep track of all open positions with detailed metrics including entry price, current value, and unrealized profit/loss in an intuitive interface.

### Active Bot Control
![Bot Control Page](images/BotControlPage.png)
Easily monitor and manage your trading bots with performance metrics, control panels, and real-time status updates for automated trading strategies.

### Trading Interface
![Trading Page](images/TradingPage.png)
Execute manual trades with a full-featured order entry system, supporting market, limit, and advanced order types with risk management controls.

### Strategy Configuration
![Strategies Page](images/StrategiesPage.png)
Configure and customize trading strategies with an intuitive parameter adjustment interface and strategy performance visualization.

### System Settings
![Settings Page](images/SettingsPage.png)
Fine-tune application preferences, notification settings, and API connection details through a comprehensive settings panel.

## Project Structure

API-Trading-Bot/
├── app/
│ ├── api/ # API endpoints
│ ├── strategies/ # Trading strategies
│ ├── utils/ # Exchange utilities
│ ├── config.py # Configuration
│ ├── main.py # App entry point
├── binance-trading-dashboard/ # React frontend application
│ ├── public/
│ ├── src/
│ │ ├── components/ # UI components
│ │ ├── contexts/ # React contexts
│ │ ├── layouts/ # Page layouts
│ │ ├── pages/ # Application pages
│ │ ├── services/ # API services
│ │ ├── utils/ # Utility functions
│ ├── Dockerfile # Frontend Docker configuration
│ ├── nginx.conf # Nginx configuration for the frontend
│ ├── package.json # Frontend dependencies
├── images/ # UI screenshots
├── tests/ # Test suite
├── Dockerfile # Backend Docker configuration
├── docker-compose.yml # Full stack Docker configuration
├── requirements.txt # Backend dependencies
├── .env # Environment variables (not tracked)
└── README.md # This file

## Contributing

Fork the repository.
Create a feature branch (git checkout -b feature/your-feature).
Commit changes (git commit -m "Add your feature").
Push to the branch (git push origin feature/your-feature).
Open a pull request.

## License

This project is licensed under the MIT License—see LICENSE for details.

## Acknowledgments

Built with Sanic, python-binance, and cbpro.
TA-Lib for technical analysis.
