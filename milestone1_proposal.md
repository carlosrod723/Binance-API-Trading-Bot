# Project Proposal: Trading Bot Backend API

## 1. Project Overview

This project aims to develop a robust and well-documented backend API for an open-source trading bot. The API will be built using Python and the Sanic framework (unless otherwise specified by the client), and it will interact with at least the Binance and Coinbase exchanges (with Alpaca Markets as a bonus). The API will provide a flexible mechanism for users to define and implement their own trading strategies using Python code and indicators from the TA-Lib library. The system will be containerized using Docker for easy deployment. Real-time updates will be provided via WebSockets.

## 2. Technical Approach

- **API Framework:** Sanic (Python)
- **Real-time Updates:** WebSockets
- **Exchanges:** Binance, Coinbase, (Bonus: Alpaca Markets)
- **Programming Language:** Python
- **Libraries:**
  - `python-binance` (or a suitable Binance API client)
  - `coinbase` (or a suitable Coinbase API client)
  - `sanic`
  - `TA-Lib`
  - `requests`
  - `websockets`
  - `python-dotenv` (for managing environment variables)
  - Any other relevant libraries that are identified.
- **Dockerization:** The application will be packaged in a Docker container for easy deployment and portability. A `Dockerfile` will be provided.
- **Authentication:** API keys will be passed in the request headers. The frontend will be responsible for securely storing and managing API keys.
- **Database:** No database will be implemented, as there are no persistence requirements.

## 3. API Endpoint Specifications

### 3.1 Start / Stop Trading Bot

- **Endpoint URL:** `/api/v1/bot/start`
- **HTTP Method:** POST
- **Request Parameters:**
  - `apiKey` (string, required): The API key for the exchange.
  - `strategyName` (string, required): The name of the strategy to use.
  - `coinPair` (string, required): The trading pair (e.g., "BTC/USDT").
  - `exchange` (string, required): The exchange to use ("binance" or "coinbase").
- **Example Request JSON:**
  ```json
  {
    "apiKey": "YOUR_API_KEY",
    "strategyName": "MyMACDStrategy",
    "coinPair": "BTC/USDT",
    "exchange": "binance"
  }
  ```
- **Response Format (Success):**
  ```json
  {
    "success": true,
    "message": "Trading bot started."
  }
  ```
- **Response Format (Error):**
  ```json
  {
    "success": false,
    "error": "Invalid API key."
  }
  ```
- **Authentication:** API key in header (`X-MBX-APIKEY`)
- **Notes:**

- **Endpoint URL:** `/api/v1/bot/stop`
- **HTTP Method:** POST
- **Request Parameters:**
  - `apiKey` (string, required): The API key for the exchange.
    - `exchange` (string, required): The exchange to use ("binance" or "coinbase").
- **Example Request JSON:**
  ```json
  {
    "apiKey": "YOUR_API_KEY",
    "exchange": "binance"
  }
  ```
- **Response Format (Success):**
  ```json
  {
    "success": true,
    "message": "Trading bot stopped."
  }
  ```
- **Response Format (Error):**
  ```json
  {
    "success": false,
    "error": "Invalid API key."
  }
  ```
- **Authentication:** API key in header (`X-MBX-APIKEY`)
- **Notes:**

### 3.2 Get Overall Balance

- **Endpoint URL:** `/api/v1/balance/overall`
- **HTTP Method:** GET
- **Request Parameters:**
  - `apiKey` (string, required): The API key for the exchange.
  - `exchange` (string, required): The exchange to use ("binance" or "coinbase").
- **Example Request JSON:** N/A - GET request
- **Response Format (Success):**
  ```json
  {
    "success": true,
    "balance": 1234.56,
    "currency": "USD"
  }
  ```
- **Response Format (Error):**
  ```json
  {
    "success": false,
    "error": "Could not retrieve balance."
  }
  ```
- **Authentication:** API key in header (`X-MBX-APIKEY`)
- **Notes:** Returns the total balance in USD equivalent.

### 3.3 Get Different Coin Balances

- **Endpoint URL:** `/api/v1/balance/coins`
- **HTTP Method:** GET
- **Request Parameters:**
  - `apiKey` (string, required): The API key for the exchange.
  - `exchange` (string, required): The exchange to use ("binance" or "coinbase").
- **Example Request JSON:** N/A - GET request
- **Response Format (Success):**
  ```json
  {
    "success": true,
    "balances": [
      {
        "coin": "BTC",
        "balance": 0.123,
        "usdValue": 456.78
      },
      {
        "coin": "ETH",
        "balance": 2.456,
        "usdValue": 789.01
      }
    ]
  }
  ```
- **Response Format (Error):**
  ```json
  {
    "success": false,
    "error": "Could not retrieve balances."
  }
  ```
- **Authentication:** API key in header (`X-MBX-APIKEY`)
- **Notes:** Returns a list of coin balances and their USD values.

### 3.4 Create Buy Order

- **Endpoint URL:** `/api/v1/order/buy`
- **HTTP Method:** POST
- **Request Parameters:**
  - `apiKey` (string, required): The API key for the exchange.
  - `coinPair` (string, required): The trading pair (e.g., "BTC/USDT").
  - `quantity` (float, required): The amount of the base currency to buy.
  - `price` (float, required): Limit price.
  - `stopLoss` (float, optional): The stop-loss price.
  - `takeProfit` (float, optional): The take-profit price.
  - `exchange` (string, required): The exchange to use ("binance" or "coinbase").
- **Example Request JSON:**
  ```json
  {
    "apiKey": "YOUR_API_KEY",
    "coinPair": "BTC/USDT",
    "quantity": 0.01,
    "price": 60000.0,
    "stopLoss": 59000.0,
    "takeProfit": 61000.0,
    "exchange": "binance"
  }
  ```
- **Response Format (Success):**
  ```json
  {
    "success": true,
    "orderId": "1234567890"
  }
  ```
- **Response Format (Error):**
  ```json
  {
    "success": false,
    "error": "Insufficient funds."
  }
  ```
- **Authentication:** API key in header (`X-MBX-APIKEY`)
- **Notes:** Returns the order ID if the order was successfully placed.

### 3.5 Create Sell Order

- **Endpoint URL:** `/api/v1/order/sell`
- **HTTP Method:** POST
- **Request Parameters:**
  - `apiKey` (string, required): The API key for the exchange.
  - `coinPair` (string, required): The trading pair (e.g., "BTC/USDT").
  - `quantity` (float, required): The amount of the base currency to sell.
  - `price` (float, required): Limit price.
  - `stopLoss` (float, optional): The stop-loss price.
  - `takeProfit` (float, optional): The take-profit price.
  - `exchange` (string, required): The exchange to use ("binance" or "coinbase").
- **Example Request JSON:**
  ```json
  {
    "apiKey": "YOUR_API_KEY",
    "coinPair": "BTC/USDT",
    "quantity": 0.01,
    "price": 65000.0,
    "stopLoss": 66000.0,
    "takeProfit": 64000.0,
    "exchange": "binance"
  }
  ```
- **Response Format (Success):**
  ```json
  {
    "success": true,
    "orderId": "9876543210"
  }
  ```
- **Response Format (Error):**
  ```json
  {
    "success": false,
    "error": "Invalid coin pair."
  }
  ```
- **Authentication:** API key in header (`X-MBX-APIKEY`)
- **Notes:** Returns the order ID if the order was successfully placed.

### 3.6 Exit Existing Position

- **Endpoint URL:** `/api/v1/order/exit`
- **HTTP Method:** POST
- **Request Parameters:**
  - `apiKey` (string, required): The API key for the exchange.
  - `coinPair` (string, required): The trading pair to exit (e.g., "BTC/USDT").
  - `exchange` (string, required): The exchange to use ("binance" or "coinbase").
- **Example Request JSON:**
  ```json
  {
    "apiKey": "YOUR_API_KEY",
    "coinPair": "BTC/USDT",
    "exchange": "binance"
  }
  ```
- **Response Format (Success):**
  ```json
  {
    "success": true,
    "message": "Position exited."
  }
  ```
- **Response Format (Error):**
  ```json
  {
    "success": false,
    "error": "No open position found for this coin pair."
  }
  ```
- **Authentication:** API key in header (`X-MBX-APIKEY`)
- **Notes:** Exits the _entire_ position for the given coin pair. Uses a market order.

### 3.7 Get List of Open Positions

- **Endpoint URL:** `/api/v1/positions/open`
- **HTTP Method:** GET
- **Request Parameters:**
  - `apiKey` (string, required): The API key for the exchange.
  - `exchange` (string, required): The exchange to use ("binance" or "coinbase").
- **Example Request JSON:** N/A - GET request
- **Response Format (Success):**
  ```json
  {
    "success": true,
    "positions": [
      {
        "coinPair": "BTC/USDT",
        "quantity": 0.01,
        "entryPrice": 60000.0,
        "stopLoss": 59000.0,
        "takeProfit": 61000.0
      }
    ]
  }
  ```
- **Response Format (Error):**
  ```json
  {
    "success": false,
    "error": "Could not retrieve open positions."
  }
  ```
- **Authentication:** API key in header (`X-MBX-APIKEY`)
- **Notes:** Returns a list of all open positions. This endpoint will use **WebSockets** for real-time updates. The initial response provides the current state, and subsequent updates are pushed via the WebSocket connection. The format of WebSocket updates will be the same as the initial response.

### 3.8 Get List of Closed Positions

- **Endpoint URL:** `/api/v1/positions/closed`
- **HTTP Method:** GET
- **Request Parameters:**
  - `apiKey` (string, required): The API key for the exchange.
  - `exchange` (string, required): The exchange to use ("binance" or "coinbase").
  - `limit` (integer, optional): The maximum number of closed positions to return (default: 100).
  - `startTime` (integer, optional): Start time for closed positions (Unix timestamp in milliseconds).
  - `endTime` (integer, optional): End time for closed positions (Unix timestamp in milliseconds).
- **Example Request JSON:** N/A - GET request
- **Response Format (Success):**
  ```json
  {
    "success": true,
    "positions": [
      {
        "coinPair": "BTC/USDT",
        "quantity": 0.01,
        "entryPrice": 60000.0,
        "exitPrice": 61000.0,
        "profit": 10.0,
        "closeTime": 1678886400000 // Unix timestamp in milliseconds
      }
    ]
  }
  ```
- **Response Format (Error):**
  ```json
  {
    "success": false,
    "error": "Could not retrieve closed positions."
  }
  ```
- **Authentication:** API key in header (`X-MBX-APIKEY`)
- **Notes:** Returns a list of closed positions. This endpoint will use **WebSockets** for real-time updates. The format of WebSocket updates will be the same as the initial response, _but only includes newly closed positions_.

### 3.9 Analyze Profit/Loss

- **Endpoint URL:** `/api/v1/profitloss`
- **HTTP Method:** GET
- **Request Parameters:**
  - `apiKey` (string, required): The API key for the exchange.
  - `exchange` (string, required): The exchange to use ("binance" or "coinbase").
- **Example Request JSON:** N/A - GET request
- **Response Format (Success):**
  ```json
  {
    "success": true,
    "totalProfit": 123.45,
    "currency": "USD"
  }
  ```
- **Response Format (Error):**
  ```json
  {
    "success": false,
    "error": "Could not retrieve profit/loss data."
  }
  ```
- **Authentication:** API key in header (`X-MBX-APIKEY`)
- **Notes:** Returns the cumulative profit/loss in USD equivalent.

### 3.10 Choose Strategy

- **Endpoint URL:** `/api/v1/strategy`
- **HTTP Method:** POST
- **Request Parameters:**
  - `apiKey` (string, required): The API key for the exchange.
  - `strategyName` (string, required): The name of the strategy to use.
- **Example Request JSON:**
  ```json
  {
    "apiKey": "YOUR_API_KEY",
    "strategyName": "MyMACDStrategy"
  }
  ```
- **Response Format (Success):**
  ```json
  {
    "success": true,
    "message": "Strategy set."
  }
  ```
- **Response Format (Error):**
  ```json
  {
    "success": false,
    "error": "Invalid strategy name."
  }
  ```
- **Authentication:** API key in header (`X-MBX-APIKEY`)
- **Notes:**

### 3.11 Choose Coin Pair

- **Endpoint URL:** `/api/v1/coinpair`
- **HTTP Method:** POST
- **Request Parameters:**
  - `apiKey` (string, required): The API key for the exchange.
  - `coinPair` (string, required): The trading pair (e.g., "BTC/USDT").
  - `exchange` (string, required): The exchange to use ("binance" or "coinbase").
- **Example Request JSON:**
  ```json
  {
    "apiKey": "YOUR_API_KEY",
    "coinPair": "BTC/USDT",
    "exchange": "binance"
  }
  ```
- **Response Format (Success):**
  ```json
  {
    "success": true,
    "message": "Coin pair set."
  }
  ```
- **Response Format (Error):**
  ```json
  {
    "success": false,
    "error": "Invalid coin pair."
  }
  ```
- **Authentication:** API key in header (`X-MBX-APIKEY`)
- **Notes:**

### 3.12 Choose Exchange

- **Endpoint URL:** `/api/v1/exchange`
- **HTTP Method:** POST
- **Request Parameters:**
  - `apiKey` (string, required): The API key for the exchange.
  - `exchange` (string, required): The exchange to use ("binance" or "coinbase").
- **Example Request JSON:**
  ```json
  {
    "apiKey": "YOUR_API_KEY",
    "exchange": "binance"
  }
  ```
- **Response Format (Success):**
  ```json
  {
    "success": true,
    "message": "Exchange set."
  }
  ```
- **Response Format (Error):**
  ```json
  {
    "success": false,
    "error": "Invalid exchange name."
  }
  ```
- **Authentication:** API key in header (`X-MBX-APIKEY`)
- **Notes:**

## 4. Strategy Builder Details

The trading bot will allow users to add their own trading strategies in Python. Strategies will be implemented as separate Python classes, inheriting from a base `Strategy` class. This base class will define the required interface, ensuring consistency and allowing the bot to easily load and use different strategies.

**Base Strategy Class (Example):**

```python
class Strategy:
    def __init__(self, indicators={}):
        self.indicators = indicators # Dictionary to store indicator values.

    def calculate_signals(self, data):
        """
        Calculates buy/sell signals based on the provided data.
        Must be implemented by subclasses.

        Args:
            data (pd.DataFrame): Historical price data (e.g., OHLCV).

        Returns:
            tuple: (buy_signal, sell_signal) -  Booleans indicating buy/sell.
                   Should return (False, False) if no action is to be taken.
        """
        raise NotImplementedError("Subclasses must implement calculate_signals().")

    def get_indicator_values(self):
        """
        Returns a dictionary of the latest indicator values.  Useful for
        logging and debugging.
        """
        values = {}
        for name, indicator in self.indicators.items():
            try:
                values[name] = indicator[-1]  # Get the last calculated value
            except (TypeError, IndexError): # Handle cases where indicator is None or empty
                values[name] = None
        return values

Example MACD Strategy:

import talib
import numpy as np
import pandas as pd

class MACDStrategy(Strategy):
    def __init__(self, fastperiod=12, slowperiod=26, signalperiod=9):
        super().__init__()  # Initialize the base class
        self.fastperiod = fastperiod
        self.slowperiod = slowperiod
        self.signalperiod = signalperiod
        self.indicators = {} # Initialize the indicators dictionary


    def calculate_signals(self, data):
        # Calculate MACD
        macd, macdsignal, macdhist = talib.MACD(
            data['close'],  # Assuming 'close' is a column in the DataFrame
            fastperiod=self.fastperiod,
            slowperiod=self.slowperiod,
            signalperiod=self.signalperiod
        )

        # Store indicator values for later access (optional, but good practice)
        self.indicators['macd'] = macd
        self.indicators['macdsignal'] = macdsignal
        self.indicators['macdhist'] = macdhist


        # Generate signals
        buy_signal = False
        sell_signal = False

        # Check for valid MACD values (avoid errors with NaN values at the start)
        if len(macd) > 0 and len(macdsignal) > 0:
            if macd[-1] > macdsignal[-1] and macd[-2] <= macdsignal[-2]:  # MACD crosses above signal line
                buy_signal = True
            elif macd[-1] < macdsignal[-1] and macd[-2] >= macdsignal[-2]: # MACD crosses below signal line
                sell_signal = True

        return buy_signal, sell_signal

Adding New Strategies:

- Create a New Python File: Create a new .py file (e.g., my_strategy.py) in a designated strategies directory (e.g., strategies/).
- Define a Strategy Class: Define a class within this file that inherits from the Strategy base class.
- Implement calculate_signals: Implement the calculate_signals method. This method must take a Pandas DataFrame (data) as input (which will contain historical price data like OHLCV - Open, High, Low, Close, Volume) and return a tuple of two booleans: (buy_signal, sell_signal).
- Use TA-Lib (Optional): Within calculate_signals, you can use TA-Lib functions to calculate technical indicators. Store any indicator values you want to track in the self.indicators dictionary.
- Instantiate Strategy The strategy can be dynamically imported using importlib.

Dynamic Strategy Loading (Conceptual):

The backend API will have a mechanism to dynamically load and use these strategy classes.  Here's a simplified conceptual example (the exact implementation will be part of the code):


import importlib
import os

def load_strategy(strategy_name, strategy_params=None):
    """Loads a strategy dynamically from the strategies directory."""
    try:
        module_name = f"strategies.{strategy_name}"
        module = importlib.import_module(module_name)

        # Find the strategy class within the module (assuming one class per file)
        for name, obj in module.__dict__.items():
          if isinstance(obj, type) and issubclass(obj, Strategy) and obj is not Strategy:
                strategy_class = obj
                break
        else:  # No Strategy subclass found
            raise ValueError(f"No Strategy subclass found in {module_name}.py")

        # Instantiate the strategy with parameters
        if strategy_params:
          strategy_instance = strategy_class(**strategy_params) # Unpack parameters
        else:
          strategy_instance = strategy_class()
        return strategy_instance

    except (ImportError, ModuleNotFoundError) as e:
        print(f"Error loading strategy {strategy_name}: {e}")
        return None

# Example usage:
# strategy = load_strategy("MyMACDStrategy") #Loads from strategies/MyMACDStrategy.py
# if strategy:
#   buy, sell = strategy.calculate_signals(historical_data)

Configurable Indicator Parameters (Conceptual):

The API will allow users to specify parameters for their chosen strategy. This could be handled in a few ways.  A simple approach would be to include a separate endpoint to set strategy parameters:

Endpoint URL: /api/v1/strategy/parameters
HTTP Method: POST
Request Parameters:
apiKey (string, required)
strategyName (string, required)
parameters (object, required): A JSON object containing the parameters for the strategy. The keys should match the parameter names expected by the strategy's __init__ method.

Example Request JSON:
{
    "apiKey": "YOUR_API_KEY",
    "strategyName": "MACDStrategy",
    "parameters": {
        "fastperiod": 12,
        "slowperiod": 26,
        "signalperiod": 9
    },
    "exchange": "binance"
}

The backend would then store these parameters (likely in memory, since we're not using a database) and pass them to the strategy's constructor when the strategy is loaded.
```
