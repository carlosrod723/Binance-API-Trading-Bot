import os
from typing import Dict
import logging
from pathlib import Path
from dotenv import load_dotenv

# Get the absolute path to the project root directory
BASE_DIR = Path(__file__).resolve().parent.parent

# Load environment variables from .env in project root
env_path = BASE_DIR / '.env'
load_dotenv(dotenv_path=env_path, override=True)

# Configure logging
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
logger = logging.getLogger(__name__)

# Print environment mode for debugging
logger.info(f"ENV_MODE from environment: {os.getenv('ENV_MODE', 'not set')}")

class Config:
    """Application configuration class for Binance trading bot."""

    def __init__(self):
        """Initialize configuration from environment variables."""
        self.ENV_MODE: str = os.getenv("ENV_MODE", "testnet")  # testnet or live
        self.LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")
        self.STRATEGIES_DIR: str = str(BASE_DIR / os.getenv("STRATEGIES_DIR", "app/strategies"))
        self.KLINES_INTERVAL: str = os.getenv("KLINES_INTERVAL", "5m")  # Stabilizes trends
        
        # Binance API credentials
        self.BINANCE_API_KEY: str = os.getenv("BINANCE_API_KEY", "")
        self.BINANCE_API_SECRET: str = os.getenv("BINANCE_API_SECRET", "")
        self.BINANCE_TESTNET_API_KEY: str = os.getenv("BINANCE_TESTNET_API_KEY", "")
        self.BINANCE_TESTNET_API_SECRET: str = os.getenv("BINANCE_TESTNET_API_SECRET", "")
        
        # WebSocket URLs
        self.BINANCE_WEBSOCKET_LIVE: str = os.getenv("BINANCE_WEBSOCKET_URL", "wss://stream.binance.com:9443/ws")
        self.BINANCE_WEBSOCKET_TESTNET: str = os.getenv("BINANCE_WEBSOCKET_TESTNET_URL", "wss://stream.testnet.binance.vision/ws")

        # Trading settings for profitability
        self.DEFAULT_TRADE_SIZE_PERCENT: float = float(os.getenv("DEFAULT_TRADE_SIZE_PERCENT", "10.0"))  # % of balance
        self.MIN_PROFIT_THRESHOLD: float = float(os.getenv("MIN_PROFIT_THRESHOLD", "0.5"))  # % profit to exit
        self.MAX_OPEN_POSITIONS: int = int(os.getenv("MAX_OPEN_POSITIONS", "3"))  # Limit risk
        
        # Risk management settings
        self.MAX_RISK_PERCENT_PER_TRADE: float = float(os.getenv("MAX_RISK_PERCENT_PER_TRADE", "1.0"))  # % risk per trade
        self.RISK_REWARD_RATIO: float = float(os.getenv("RISK_REWARD_RATIO", "2.0"))  # Target R:R ratio
        self.TRAILING_STOP_ACTIVATION: float = float(os.getenv("TRAILING_STOP_ACTIVATION", "1.0"))  # When to activate trailing stop

        # ========== ADD THESE NEW API RATE LIMITING SETTINGS ==========
        
        # API Rate Limiting settings
        self.API_MAX_REQUESTS_PER_MINUTE: int = int(os.getenv("API_MAX_REQUESTS_PER_MINUTE", "1000"))  # Actual limit is 1200
        self.API_MAX_REQUESTS_PER_SECOND: int = int(os.getenv("API_MAX_REQUESTS_PER_SECOND", "15"))  # Actual limit is 20
        self.API_WEIGHT_LIMIT_PER_MINUTE: int = int(os.getenv("API_WEIGHT_LIMIT_PER_MINUTE", "1000"))  # Binance weight limit
        
        # Safety buffer (%) to stay under the limits
        self.API_RATE_LIMIT_BUFFER: float = float(os.getenv("API_RATE_LIMIT_BUFFER", "20.0"))  # Stay 20% under limits
        
        # Retry settings
        self.API_MAX_RETRIES: int = int(os.getenv("API_MAX_RETRIES", "3"))
        self.API_RETRY_DELAY: float = float(os.getenv("API_RETRY_DELAY", "2.0"))  # Base delay in seconds
        self.API_RETRY_MAX_DELAY: float = float(os.getenv("API_RETRY_MAX_DELAY", "60.0"))  # Maximum delay
        
        # Cached data TTL in seconds for different endpoints
        self.CACHE_TTL_KLINES: int = int(os.getenv("CACHE_TTL_KLINES", "30"))  # Default 30 seconds for klines
        self.CACHE_TTL_MARKET_DATA: int = int(os.getenv("CACHE_TTL_MARKET_DATA", "15"))  # 15 seconds for market data
        self.CACHE_TTL_ACCOUNT: int = int(os.getenv("CACHE_TTL_ACCOUNT", "60"))  # 60 seconds for account data
        self.CACHE_TTL_ORDERS: int = int(os.getenv("CACHE_TTL_ORDERS", "5"))  # 5 seconds for order data
        self.CACHE_TTL_SYMBOLS: int = int(os.getenv("CACHE_TTL_SYMBOLS", "3600"))  # 1 hour for symbol info
        
        # Dynamic TTL settings based on timeframe
        self.CACHE_TTL_MULTIPLIER_1M: float = float(os.getenv("CACHE_TTL_MULTIPLIER_1M", "0.5"))  # 1m klines cache for 0.5 * interval seconds
        self.CACHE_TTL_MULTIPLIER_1H: float = float(os.getenv("CACHE_TTL_MULTIPLIER_1H", "0.2"))  # 1h klines cache for 0.2 * interval seconds
        self.CACHE_TTL_MULTIPLIER_1D: float = float(os.getenv("CACHE_TTL_MULTIPLIER_1D", "0.1"))  # 1d klines cache for 0.1 * interval seconds
        
        # Request throttling and batching
        self.REQUEST_THROTTLE_MS: int = int(os.getenv("REQUEST_THROTTLE_MS", "100"))  # Minimum ms between requests
        self.BATCH_POSITION_UPDATES: bool = os.getenv("BATCH_POSITION_UPDATES", "true").lower() == "true"  # Batch position updates
        self.MAX_CONCURRENT_REQUESTS: int = int(os.getenv("MAX_CONCURRENT_REQUESTS", "5"))  # Maximum concurrent requests
        
        # WebSocket settings for reducing connection overhead
        self.WS_RECONNECT_INTERVAL: int = int(os.getenv("WS_RECONNECT_INTERVAL", "5000"))  # ms between reconnection attempts
        self.WS_MAX_RECONNECT_ATTEMPTS: int = int(os.getenv("WS_MAX_RECONNECT_ATTEMPTS", "5"))  # max reconnection attempts
        self.WS_PING_INTERVAL: int = int(os.getenv("WS_PING_INTERVAL", "60000"))  # ms between ping messages
        self.WS_CONNECTION_TIMEOUT: int = int(os.getenv("WS_CONNECTION_TIMEOUT", "30000"))  # ms to wait for connection
        
        # Scanner throttling
        self.SCANNER_MIN_INTERVAL: int = int(os.getenv("SCANNER_MIN_INTERVAL", "300"))  # Minimum seconds between scanner runs

        logger.info(f"Initialized with ENV_MODE: {self.ENV_MODE}")
        logger.info(f"Using API key starting with: {self.get_binance_credentials()[0][:5] if self.get_binance_credentials()[0] else 'None'}...")

    def get_binance_credentials(self) -> tuple[str, str]:
        if self.ENV_MODE == "testnet":
            logger.info("Using Testnet credentials")
            return self.BINANCE_TESTNET_API_KEY, self.BINANCE_TESTNET_API_SECRET
        logger.info("Using Live credentials")
        return self.BINANCE_API_KEY, self.BINANCE_API_SECRET

    def get_websocket_url(self) -> str:
        """Get active Binance WebSocket URL based on ENV_MODE."""
        return self.BINANCE_WEBSOCKET_TESTNET if self.ENV_MODE == "testnet" else self.BINANCE_WEBSOCKET_LIVE

    def is_production(self) -> bool:
        """Helper to check if running in production mode."""
        return self.ENV_MODE == "live"
    
    def get_cache_ttl(self, data_type: str, interval: str = None) -> int:
        """Get cache TTL for a specific data type and interval.
        
        Args:
            data_type: Type of data (klines, market_data, account, orders, symbols)
            interval: Time interval for klines data (1m, 5m, 1h, etc.)
            
        Returns:
            TTL in seconds
        """
        base_ttl = {
            'klines': self.CACHE_TTL_KLINES,
            'market_data': self.CACHE_TTL_MARKET_DATA,
            'account': self.CACHE_TTL_ACCOUNT,
            'orders': self.CACHE_TTL_ORDERS,
            'symbols': self.CACHE_TTL_SYMBOLS
        }.get(data_type.lower(), 30)  # Default 30 seconds
        
        # Adjust TTL for klines based on interval
        if data_type.lower() == 'klines' and interval:
            # Parse interval (e.g., "5m" -> 5 minutes, "1h" -> 1 hour)
            if interval.endswith('m'):
                # Minutes interval
                mins = int(interval[:-1])
                return int(mins * 60 * self.CACHE_TTL_MULTIPLIER_1M)
            elif interval.endswith('h'):
                # Hours interval
                hours = int(interval[:-1])
                return int(hours * 3600 * self.CACHE_TTL_MULTIPLIER_1H)
            elif interval.endswith('d'):
                # Days interval
                days = int(interval[:-1])
                return int(days * 86400 * self.CACHE_TTL_MULTIPLIER_1D)
        
        return base_ttl
    
    def get_rate_limit_settings(self) -> Dict:
        """Get all rate limit settings as a dictionary."""
        return {
            'max_requests_per_minute': self.API_MAX_REQUESTS_PER_MINUTE,
            'max_requests_per_second': self.API_MAX_REQUESTS_PER_SECOND,
            'weight_limit_per_minute': self.API_WEIGHT_LIMIT_PER_MINUTE,
            'buffer_percent': self.API_RATE_LIMIT_BUFFER,
            'max_retries': self.API_MAX_RETRIES,
            'retry_delay': self.API_RETRY_DELAY,
            'retry_max_delay': self.API_RETRY_MAX_DELAY,
            'throttle_ms': self.REQUEST_THROTTLE_MS,
            'max_concurrent': self.MAX_CONCURRENT_REQUESTS
        }

    def validate_config(self, strict: bool = False) -> bool:
        """Validate configuration settings."""
        missing_vars = []
        active_key, active_secret = self.get_binance_credentials()

        if not active_key:
            missing_vars.append("BINANCE_TESTNET_API_KEY" if self.ENV_MODE == "testnet" else "BINANCE_API_KEY")
        if not active_secret:
            missing_vars.append("BINANCE_TESTNET_API_SECRET" if self.ENV_MODE == "testnet" else "BINANCE_API_SECRET")
        if self.DEFAULT_TRADE_SIZE_PERCENT <= 0 or self.DEFAULT_TRADE_SIZE_PERCENT > 100:
            missing_vars.append("DEFAULT_TRADE_SIZE_PERCENT (must be 0-100)")
        if self.MIN_PROFIT_THRESHOLD < 0:
            missing_vars.append("MIN_PROFIT_THRESHOLD (must be non-negative)")
        if self.MAX_OPEN_POSITIONS <= 0:
            missing_vars.append("MAX_OPEN_POSITIONS (must be positive)")
        if self.MAX_RISK_PERCENT_PER_TRADE <= 0 or self.MAX_RISK_PERCENT_PER_TRADE > 5:
            missing_vars.append("MAX_RISK_PERCENT_PER_TRADE (must be 0-5)")
        if self.RISK_REWARD_RATIO <= 0:
            missing_vars.append("RISK_REWARD_RATIO (must be positive)")
        if self.TRAILING_STOP_ACTIVATION <= 0:
            missing_vars.append("TRAILING_STOP_ACTIVATION (must be positive)")
            
        # Validate API rate limit settings
        if self.API_MAX_REQUESTS_PER_MINUTE <= 0:
            missing_vars.append("API_MAX_REQUESTS_PER_MINUTE (must be positive)")
        if self.API_MAX_REQUESTS_PER_SECOND <= 0:
            missing_vars.append("API_MAX_REQUESTS_PER_SECOND (must be positive)")
        if self.API_WEIGHT_LIMIT_PER_MINUTE <= 0:
            missing_vars.append("API_WEIGHT_LIMIT_PER_MINUTE (must be positive)")
        if self.API_RATE_LIMIT_BUFFER < 0 or self.API_RATE_LIMIT_BUFFER >= 100:
            missing_vars.append("API_RATE_LIMIT_BUFFER (must be 0-99)")
        if self.API_MAX_RETRIES < 0:
            missing_vars.append("API_MAX_RETRIES (must be non-negative)")
        if self.API_RETRY_DELAY <= 0:
            missing_vars.append("API_RETRY_DELAY (must be positive)")
        if self.API_RETRY_MAX_DELAY <= 0:
            missing_vars.append("API_RETRY_MAX_DELAY (must be positive)")
            
        # Validate cache TTL settings
        if self.CACHE_TTL_KLINES < 0:
            missing_vars.append("CACHE_TTL_KLINES (must be non-negative)")
        if self.CACHE_TTL_MARKET_DATA < 0:
            missing_vars.append("CACHE_TTL_MARKET_DATA (must be non-negative)")
        if self.CACHE_TTL_ACCOUNT < 0:
            missing_vars.append("CACHE_TTL_ACCOUNT (must be non-negative)")
        if self.CACHE_TTL_ORDERS < 0:
            missing_vars.append("CACHE_TTL_ORDERS (must be non-negative)")
        if self.CACHE_TTL_SYMBOLS < 0:
            missing_vars.append("CACHE_TTL_SYMBOLS (must be non-negative)")

        if missing_vars:
            message = f"Invalid config: {', '.join(missing_vars)}"
            if strict:
                raise ValueError(message)
            logger.warning(message)
            return False
        logger.info("Configuration validated successfully")
        return True

# Singleton instance
config = Config()

# Validate on import (non-strict)
config.validate_config(strict=False)