# app/utils/binance_utils.py

import logging
import time
import traceback
import asyncio
import hashlib
import hmac
import json
import uuid
from app.config import config
from datetime import datetime, timedelta
from binance import AsyncClient
from binance.exceptions import BinanceAPIException
from decimal import Decimal, InvalidOperation, ROUND_DOWN
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from typing import Optional, List, Dict, Union, Any, Tuple, Set
from app.config import config

logger = logging.getLogger(__name__)

class BinanceClient:
    """Enhanced async wrapper for Binance spot API interactions."""

    def __init__(self, api_key: str, api_secret: str, testnet: bool = False):
        """Initialize with API credentials.
        
        Args:
            api_key: The Binance API key
            api_secret: The Binance API secret
            testnet: Whether to use the testnet
        
        Raises:
            ValueError: If API credentials are missing
        """
        self.api_key = api_key
        self.api_secret = api_secret
        self.testnet = testnet or config.ENV_MODE == "testnet"
        self.client = None
        self.initialized = False
        self.last_request_time = 0
        self.request_count = 0
        self.request_weight = 0
        self.last_error_time = 0
        self._ws_connections = {}
        self._symbol_info_cache = {}
        self._symbol_precision_cache = {}
        self._last_server_time_diff = 0
        self._health_check_timestamp = 0
        self._account_update_listeners = set()
        self._order_update_listeners = set()
        self._kline_update_listeners = {}
        self._trade_pair_listeners = {}

        # Diagnostics
        logger.info(f"BinanceClient initialized with:")
        logger.info(f"  - Testnet mode: {self.testnet}")
        logger.info(f"  - API Key: {api_key[:5]}...{api_key[-5:] if len(api_key) > 10 else ''}")
        logger.info(f"  - API Key length: {len(api_key)} characters")
        
        if not self.api_key or not self.api_secret:
            logger.error("Missing Binance API credentials")
            raise ValueError("API key and secret are required")
        
        logger.debug("BinanceClient initialized with testnet=%s", self.testnet)

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=1, max=60),
        retry=retry_if_exception_type((ConnectionError, TimeoutError, BinanceAPIException))
    )
    async def initialize(self) -> None:
        """Initialize the Binance async client with exponential backoff retry.
        
        Raises:
            Exception: If client initialization fails after retries
        """
        if self.initialized and self.client:
            logger.debug("Client already initialized")
            return
            
        try:
            logger.info("Initializing Binance client (testnet=%s)", self.testnet)
            
            # Create the client (without request_timeout which is not supported in newer versions)
            self.client = await AsyncClient.create(
                api_key=self.api_key, 
                api_secret=self.api_secret, 
                testnet=self.testnet
            )
            
            # Verify connection with multiple retries for reliability
            max_account_retries = 3
            account_verified = False
            last_account_error = None
            
            for attempt in range(1, max_account_retries + 1):
                try:
                    # Test connection by fetching account info
                    await self.client.get_account()
                    account_verified = True
                    logger.info(f"Successfully verified account access on attempt {attempt}")
                    break
                except Exception as e:
                    last_account_error = e
                    logger.warning(f"Account verification attempt {attempt}/{max_account_retries} failed: {str(e)}")
                    if attempt < max_account_retries:
                        await asyncio.sleep(2 * attempt)  # Progressive backoff
            
            if not account_verified:
                logger.error(f"Failed to verify account access after {max_account_retries} attempts: {str(last_account_error)}")
                # Don't raise here - we'll try to recover and some methods might still work
            
            # Synchronize time with server (important for signed requests)
            try:
                await self._sync_server_time()
            except Exception as e:
                logger.warning(f"Time synchronization failed: {str(e)}")
            
            self.initialized = True
            self._health_check_timestamp = time.time()
            
            logger.info("Binance client initialized successfully")
            
        except BinanceAPIException as e:
            self.last_error_time = time.time()
            # Handle specific Binance API errors
            if e.code == -2015:  # Invalid API key
                logger.error("Invalid API key or secret: %s", str(e))
                raise ValueError(f"Invalid API credentials: {str(e)}")
                
            elif e.code == -1021:  # Timestamp out of sync
                logger.error("Timestamp out of sync with Binance servers: %s", str(e))
                # Force time sync and retry
                try:
                    if self.client:
                        server_time = await self.client.get_server_time()
                        logger.info(f"Retrieved server time: {server_time}")
                except Exception as sync_error:
                    logger.error(f"Failed to get server time during recovery: {str(sync_error)}")
                # Retry after logging
                raise ConnectionError(f"Time synchronization error: {str(e)}")
                
            elif e.code == -1003:  # Too many requests weight
                logger.error("Rate limit exceeded: %s", str(e))
                # This needs a longer backoff
                raise TimeoutError(f"Rate limit exceeded: {str(e)}")

            elif e.code == -1000:  # Unknown error
                logger.error(f"Unknown Binance error: {str(e)}")
                # This is potentially retryable - network issues often cause this
                raise ConnectionError(f"Unknown Binance error (potentially network issue): {str(e)}")
                
            else:
                logger.error("Binance API error: [%s] %s", e.code, str(e))
                # Most API errors should be retried a few times
                raise ConnectionError(f"Binance API error [{e.code}]: {str(e)}")
                
        except ConnectionError as e:
            self.last_error_time = time.time()
            logger.error("Connection error initializing Binance client: %s", str(e))
            logger.debug("Connection error traceback: %s", traceback.format_exc())
            self.client = None
            self.initialized = False
            raise
                
        except Exception as e:
            self.last_error_time = time.time()
            logger.error("Error initializing Binance client: %s", str(e))
            logger.debug("Initialization error traceback: %s", traceback.format_exc())
            self.client = None
            self.initialized = False
            raise

    async def close(self) -> None:
        """Close the Binance client connection safely."""
        try:
            # Close all WebSocket connections
            await self._close_all_ws_connections()
            
            # Close main client connection
            if self.client:
                logger.info("Closing Binance client connection")
                await self.client.close_connection()
                logger.info("Binance client connection closed")
        except Exception as e:
            logger.error("Error closing Binance client: %s", str(e))
        finally:
            self.client = None
            self.initialized = False
            
    def get_health_metrics(self) -> Dict[str, Any]:
        """Get comprehensive client health metrics.
        
        Returns:
            Dictionary with detailed health metrics
        """
        current_time = time.time()
        
        # Base metrics
        metrics = {
            "initialized": self.initialized,
            "testnet": self.testnet,
            "timeSyncDiff": round(self._last_server_time_diff, 3) if hasattr(self, '_last_server_time_diff') else None,
            "lastRequestAge": round(current_time - self.last_request_time, 1) if self.last_request_time > 0 else None,
            "lastErrorAge": round(current_time - self.last_error_time, 1) if hasattr(self, 'last_error_time') and self.last_error_time > 0 else None,
            "requestCount": self.request_count,
            "requestWeight": self.request_weight,
            "activeConnections": len(self._ws_connections) if hasattr(self, '_ws_connections') else 0,
            "timestamp": int(current_time * 1000)
        }
        
        # Add rate limit information if available
        if hasattr(self, 'rate_limits'):
            metrics["rateLimits"] = {
                window: {
                    "current": data["weight"],
                    "limit": data["limit"],
                    "utilization": round(data["weight"] / data["limit"] * 100, 1) if data["limit"] > 0 else 0,
                    "reset_in": round(data["window"] - (current_time - data["last_reset"]), 1) if current_time > data["last_reset"] else 0
                }
                for window, data in self.rate_limits.items()
            }
        
        # Add health check information
        if hasattr(self, '_health_check_timestamp'):
            metrics["lastHealthCheck"] = {
                "age": round(current_time - self._health_check_timestamp, 1),
                "timestamp": int(self._health_check_timestamp * 1000)
            }
        
        # Add consecutive failures if any
        if hasattr(self, '_consecutive_init_failures') and self._consecutive_init_failures > 0:
            metrics["consecutiveInitFailures"] = self._consecutive_init_failures
        
        # Add cache metrics if available
        if hasattr(self, '_symbol_info_cache'):
            metrics["cacheSize"] = {
                "symbols": len(self._symbol_info_cache) if self._symbol_info_cache else 0
            }
        
        return metrics

    async def _sync_server_time(self) -> None:
        """Synchronize local time with Binance server time."""
        try:
            server_time = await self.client.get_server_time()
            server_timestamp = server_time['serverTime'] / 1000  # Convert to seconds
            local_time = time.time()
            time_diff = server_timestamp - local_time
            
            self._last_server_time_diff = time_diff
            
            if abs(time_diff) > 1:
                logger.warning(f"Time difference with Binance server: {time_diff:.2f} seconds")
                
            logger.debug(f"Server time synchronization completed. Difference: {time_diff:.2f}s")
            
        except Exception as e:
            logger.error(f"Failed to synchronize server time: {str(e)}")

    async def _ensure_initialized(self) -> None:
        """Ensure the client is initialized before making requests with improved reliability."""
        # Attempt initialization if not initialized
        if not self.initialized or not self.client:
            try:
                logger.info("Client not initialized. Performing initialization...")
                await self.initialize()
            except Exception as e:
                logger.error(f"Failed to initialize client: {str(e)}")
                # Set a flag to indicate repeated failures
                if not hasattr(self, '_consecutive_init_failures'):
                    self._consecutive_init_failures = 0
                self._consecutive_init_failures += 1
                # Implement exponential backoff for retries based on consecutive failures
                if self._consecutive_init_failures > 3:
                    logger.critical(f"Multiple consecutive initialization failures: {self._consecutive_init_failures}")
                raise
        else:
            # Reset failure counter on successful initialization
            if hasattr(self, '_consecutive_init_failures') and self._consecutive_init_failures > 0:
                logger.info(f"Resetting consecutive initialization failures from {self._consecutive_init_failures} to 0")
                self._consecutive_init_failures = 0
        
        # Periodic health check with adaptive frequency based on previous errors
        current_time = time.time()
        health_check_interval = 1800  # Default: 30 minutes (1800 seconds)
        
        # Reduce interval if we've seen recent errors
        if hasattr(self, 'last_error_time') and self.last_error_time > 0:
            time_since_error = current_time - self.last_error_time
            if time_since_error < 300:  # Less than 5 minutes since last error
                health_check_interval = 300  # Check every 5 minutes
            elif time_since_error < 1800:  # Less than 30 minutes
                health_check_interval = 600  # Check every 10 minutes
                
        if (not hasattr(self, '_health_check_timestamp') or 
                current_time - self._health_check_timestamp > health_check_interval):
            logger.debug(f"Performing periodic health check (interval: {health_check_interval}s)")
            try:
                # More thorough health check
                if self.client:
                    # First ping
                    await self.client.ping()
                    
                    # Server time check (helps with timestamp sync issues)
                    await self._sync_server_time()
                    
                    # Light API call to verify actual endpoint access
                    exchange_info = await self.client.get_exchange_info()
                    if not exchange_info or not isinstance(exchange_info, dict):
                        raise ValueError("Invalid exchange info response format")
                        
                    self._health_check_timestamp = current_time
                    logger.info("Health check completed successfully")
            except Exception as e:
                logger.warning(f"Health check failed: {str(e)}")
                # If health check fails, try to reinitialize
                self.initialized = False
                try:
                    await self.initialize()
                    logger.info("Re-initialization after failed health check succeeded")
                except Exception as reinit_error:
                    logger.error(f"Re-initialization after health check failed: {str(reinit_error)}")
                    # Don't raise - let the caller handle this by checking self.initialized

    async def _track_request(self, weight: int = 1) -> None:
        """Track API request for rate limiting with progressive backoff."""
        current_time = time.time()
        
        # Track multiple rate limit windows
        if not hasattr(self, 'rate_limits'):
            self.rate_limits = {
                'minute': {'window': 60, 'limit': 1200, 'weight': 0, 'last_reset': current_time},
                'second': {'window': 1, 'limit': 50, 'weight': 0, 'last_reset': current_time},
                'day': {'window': 86400, 'limit': 100000, 'weight': 0, 'last_reset': current_time}
            }
        
        # Update all rate limit windows
        for window_name, data in self.rate_limits.items():
            # Reset counter if window has passed
            if current_time - data['last_reset'] > data['window']:
                data['weight'] = 0
                data['last_reset'] = current_time
            
            # Add current request weight
            data['weight'] += weight
            
            # Progressive backoff as we approach limits
            limit_percentage = data['weight'] / data['limit']
            
            # Apply increasingly aggressive throttling as we approach limits
            if limit_percentage > 0.8:
                # Calculate backoff time: more aggressive as we get closer to limit
                backoff_factor = min(1.0, (limit_percentage - 0.8) * 5)  # 0 at 80%, 1.0 at 100%
                backoff_time = data['window'] * backoff_factor * 0.2  # Up to 20% of the window
                
                if backoff_time > 0:
                    logger.warning(f"Rate limit threshold ({window_name}): {data['weight']}/{data['limit']} "
                                  f"({limit_percentage:.1%}). Backing off for {backoff_time:.2f}s")
                    await asyncio.sleep(backoff_time)
        
        self.last_request_time = current_time
        self.request_count += 1
        self.request_weight += weight
        
        # Only log every 10th request to reduce log spam
        if self.request_count % 10 == 0:
            logger.debug(f"API request tracked: count={self.request_count}, weight={self.request_weight}")

    async def _get_symbol_info_cached(self, symbol: str) -> Dict[str, Any]:
        """Get symbol information with optimized caching.
        
        Args:
            symbol: The trading pair symbol
        
        Returns:
            Symbol information dictionary or None if not available
        """
        current_time = time.time()
        
        # Initialize cache if it doesn't exist
        if not hasattr(self, '_symbol_info_cache'):
            self._symbol_info_cache = {}
            self._last_symbols_refresh = 0
            self._all_symbols_cache = None
        
        # Periodic full refresh of all symbols (once every 12 hours)
        # This reduces individual symbol lookups by having a complete cache
        if current_time - self._last_symbols_refresh > 43200:
            try:
                # Get exchange info for all symbols at once (weight=10 but saves many individual calls)
                await self._track_request(weight=10)
                info = await self.client.get_exchange_info()
                
                # Update cache for all symbols
                for symbol_info in info.get('symbols', []):
                    symbol_name = symbol_info.get('symbol')
                    if symbol_name:
                        self._symbol_info_cache[symbol_name] = {
                            'info': symbol_info,
                            'timestamp': current_time
                        }
                
                self._last_symbols_refresh = current_time
                self._all_symbols_cache = info
                logger.info(f"Refreshed cache for all {len(self._symbol_info_cache)} symbols")
                
            except Exception as e:
                logger.error(f"Error refreshing symbols cache: {str(e)}")
        
        # Return cached data if available and not expired
        if symbol in self._symbol_info_cache:
            cache_entry = self._symbol_info_cache[symbol]
            # Cache valid for 24 hours
            if current_time - cache_entry.get('timestamp', 0) < 86400:
                return cache_entry.get('info')
        
        # If we get here, either symbol not in cache or cache expired
        # Make individual request
        try:
            await self._track_request(weight=1)
            info = await self.client.get_symbol_info(symbol)
            if info:
                self._symbol_info_cache[symbol] = {
                    'info': info,
                    'timestamp': current_time
                }
                return info
        except Exception as e:
            logger.warning(f"Error fetching symbol info for {symbol}: {str(e)}")
            # Return expired cache as fallback if available
            if symbol in self._symbol_info_cache:
                logger.warning(f"Using expired cache for {symbol} as fallback")
                return self._symbol_info_cache[symbol].get('info')
        
        return None

    async def _adjust_quantity_precision(self, symbol: str, quantity: float) -> float:
        """Adjust quantity to symbol's precision.
        
        Args:
            symbol: The trading pair symbol
            quantity: The quantity to adjust
        
        Returns:
            The adjusted quantity with proper precision
        
        Raises:
            ValueError: If quantity adjustment fails
        """
        await self._ensure_initialized()
        
        try:
            # Get cached symbol info
            info = await self._get_symbol_info_cached(symbol)
            
            if not info:
                logger.warning("Symbol info not found for %s, using default precision", symbol)
                # Default precision if we can't get symbol info
                precision = 8
                min_qty = 0
                max_qty = float('inf')
                step_size = 0
            else:
                # Extract precision from symbol info
                precision = info.get("baseAssetPrecision", 8)
                
                # Find lot size filter
                lot_size_filter = None
                for filter_item in info.get("filters", []):
                    if filter_item.get("filterType") == "LOT_SIZE":
                        lot_size_filter = filter_item
                        break
                        
                if lot_size_filter:
                    min_qty = float(lot_size_filter.get("minQty", 0))
                    max_qty = float(lot_size_filter.get("maxQty", float('inf')))
                    step_size = float(lot_size_filter.get("stepSize", 0))
                else:
                    min_qty = 0
                    max_qty = float('inf')
                    step_size = 0
                    
                # Cache precision info separately for quick access
                if not hasattr(self, '_symbol_precision_cache'):
                    self._symbol_precision_cache = {}
                    
                self._symbol_precision_cache[symbol] = {
                    'precision': precision,
                    'minQty': min_qty,
                    'maxQty': max_qty,
                    'stepSize': step_size,
                    'timestamp': time.time()
                }
            
            # Use Decimal for precise rounding
            decimal_qty = Decimal(str(quantity))
            
            # Ensure quantity meets minimum
            if quantity < min_qty:
                logger.warning("Quantity %f below minimum %f for %s", quantity, min_qty, symbol)
                adjusted = min_qty
            # Ensure quantity doesn't exceed maximum
            elif quantity > max_qty:
                logger.warning("Quantity %f above maximum %f for %s", quantity, max_qty, symbol)
                adjusted = max_qty
            # Adjust according to step size
            elif step_size > 0:
                # Calculate how many steps
                step_decimal = Decimal(str(step_size))
                steps = decimal_qty / step_decimal
                # Round down to nearest step
                steps_int = int(steps)
                # Calculate adjusted quantity
                adjusted = float(Decimal(str(steps_int)) * step_decimal)
                # Apply precision
                adjusted = float(decimal_qty.quantize(
                    Decimal('0.' + '0' * precision), 
                    rounding=ROUND_DOWN
                ))
            else:
                # Just apply precision
                adjusted = float(decimal_qty.quantize(
                    Decimal('0.' + '0' * precision),
                    rounding=ROUND_DOWN
                ))
            
            logger.debug("Adjusted quantity for %s: %f -> %f", symbol, quantity, adjusted)
            return adjusted
        
        except BinanceAPIException as e:
            logger.error("Binance API error adjusting precision: [%s] %s", e.code, str(e))
            raise
        
        except (InvalidOperation, ValueError) as e:
            logger.error("Error adjusting precision for %s: %s", symbol, str(e))
            raise ValueError(f"Invalid quantity format: {str(e)}")
        
        except Exception as e:
            logger.error("Error adjusting precision for %s: %s", symbol, str(e))
            logger.debug("Precision adjustment error traceback: %s", traceback.format_exc())
            return quantity
    
    async def _adjust_price_precision(self, symbol: str, price: float) -> float:
        """Adjust price to symbol's price precision.
        
        Args:
            symbol: The trading pair symbol
            price: The price to adjust
            
        Returns:
            The adjusted price with proper precision
        """
        await self._ensure_initialized()
        
        try:
            # Check cache first
            if symbol in self._symbol_precision_cache and 'pricePrecision' in self._symbol_precision_cache[symbol]:
                price_precision = self._symbol_precision_cache[symbol]['pricePrecision']
            else:
                # Get symbol info
                await self._track_request()
                info = await self.client.get_symbol_info(symbol)
                
                if not info:
                    logger.warning("Symbol info not found for %s", symbol)
                    return price
                
                price_precision = info.get("quotePrecision", 8)
                
                # Find price filter
                price_filter = None
                for filter_item in info.get("filters", []):
                    if filter_item.get("filterType") == "PRICE_FILTER":
                        price_filter = filter_item
                        break
                
                # Update cache
                if symbol in self._symbol_precision_cache:
                    self._symbol_precision_cache[symbol]['pricePrecision'] = price_precision
                    if price_filter:
                        self._symbol_precision_cache[symbol]['minPrice'] = float(price_filter.get("minPrice", 0))
                        self._symbol_precision_cache[symbol]['maxPrice'] = float(price_filter.get("maxPrice", float('inf')))
                        self._symbol_precision_cache[symbol]['tickSize'] = float(price_filter.get("tickSize", 0))
                else:
                    cache_entry = {
                        'pricePrecision': price_precision,
                        'timestamp': time.time()
                    }
                    if price_filter:
                        cache_entry['minPrice'] = float(price_filter.get("minPrice", 0))
                        cache_entry['maxPrice'] = float(price_filter.get("maxPrice", float('inf')))
                        cache_entry['tickSize'] = float(price_filter.get("tickSize", 0))
                    self._symbol_precision_cache[symbol] = cache_entry
            
            # Apply price constraints from cache
            if 'minPrice' in self._symbol_precision_cache[symbol]:
                min_price = self._symbol_precision_cache[symbol]['minPrice']
                if price < min_price:
                    logger.warning("Price %f below minimum %f for %s", price, min_price, symbol)
                    price = min_price
            
            if 'maxPrice' in self._symbol_precision_cache[symbol]:
                max_price = self._symbol_precision_cache[symbol]['maxPrice']
                if price > max_price:
                    logger.warning("Price %f above maximum %f for %s", price, max_price, symbol)
                    price = max_price
            
            # Apply tick size if available
            if 'tickSize' in self._symbol_precision_cache[symbol]:
                tick_size = self._symbol_precision_cache[symbol]['tickSize']
                if tick_size > 0:
                    price_decimal = Decimal(str(price))
                    tick_decimal = Decimal(str(tick_size))
                    ticks = price_decimal / tick_decimal
                    ticks_int = int(ticks)
                    price = float(Decimal(str(ticks_int)) * tick_decimal)
            
            # Apply precision
            adjusted = float(Decimal(str(price)).quantize(
                Decimal('0.' + '0' * price_precision),
                rounding=ROUND_DOWN
            ))
            
            logger.debug("Adjusted price for %s: %f -> %f", symbol, price, adjusted)
            return adjusted
            
        except Exception as e:
            logger.error("Error adjusting price precision for %s: %s", symbol, str(e))
            return price

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type((ConnectionError, TimeoutError))
    )
    async def get_account_balance(self) -> Optional[float]:
        """Get total USDT balance.
        
        Returns:
            The total USDT balance or None if unavailable
            
        Raises:
            ConnectionError: If connection to Binance fails
            Exception: For other errors
        """
        await self._ensure_initialized()
        
        try:
            # Ensure we have a client
            if not self.client:
                logger.error("Binance client is not initialized")
                return None
            
            # Get account information
            account = await self.client.get_account()
            
            if not account or "balances" not in account:
                logger.error("Invalid account data returned from Binance")
                return None
                
            total_balance = 0.0
            for asset in account.get("balances", []):
                if asset["asset"] == "USDT":
                    free = float(asset["free"])
                    locked = float(asset["locked"])
                    total_balance += free + locked
                    
            logger.debug("Total USDT balance: %f", total_balance)
            return total_balance if total_balance > 0 else 0.0
            
        except Exception as e:
            logger.error("Error getting balance: %s", str(e))
            logger.debug("Get balance error traceback: %s", traceback.format_exc())
            return None

    async def get_coin_balances(self) -> Optional[List[Dict[str, Union[str, float]]]]:
        """Get individual coin balances.
        
        Returns:
            List of coin balances or None if unavailable
        """
        try:
            # Get account information
            await self._ensure_initialized()
            account = await self.client.get_account()
            
            if not account or "balances" not in account:
                logger.error("Invalid account data returned from Binance")
                return None
                
            # Filter for non-zero balances
            valid_balances = []
            
            for asset in account.get("balances", []):
                free = float(asset["free"])
                locked = float(asset["locked"])
                total = free + locked
                
                if total > 0:
                    # Create balance data
                    balance_data = {
                        "coin": asset["asset"],
                        "free": free,
                        "locked": locked,
                        "balance": total,
                        "usdValue": total if asset["asset"] == "USDT" else 0.0
                    }
                    valid_balances.append(balance_data)
            
            # Sort by balance (descending)
            valid_balances.sort(key=lambda x: x["balance"], reverse=True)
            
            logger.info(f"Fetched {len(valid_balances)} coin balances")
            return valid_balances
            
        except Exception as e:
            logger.error(f"Error getting coin balances: {str(e)}")
            return None

    async def _process_balance_with_value(self, balance_data: Dict[str, Any]) -> Dict[str, Any]:
        """Process a single balance entry to add USDT value.
        
        Args:
            balance_data: Balance data to process
            
        Returns:
            Balance data with USDT value added
        """
        try:
            coin = balance_data["coin"]
            amount = balance_data["balance"]
            
            usd_value = await self._get_usdt_value(coin, amount)
            balance_data["usdValue"] = usd_value if usd_value is not None else 0.0
            
            return balance_data
        except Exception as e:
            logger.warning("Error processing balance for %s: %s", 
                          balance_data.get("coin", "unknown"), str(e))
            balance_data["usdValue"] = 0.0
            return balance_data

    async def _get_usdt_value_safe(self, coin: str, amount: float) -> Optional[float]:
        """Safely get USDT value of a coin with error handling.
        
        Args:
            coin: The coin symbol
            amount: The amount of coins
            
        Returns:
            The USDT value or None if unavailable
        """
        if amount <= 0:
            return 0.0
            
        try:
            # Try direct USDT pair
            pair = f"{coin}USDT"
            
            await self._track_request()
            ticker = await self.client.get_symbol_ticker(symbol=pair)
            if ticker and "price" in ticker:
                price = float(ticker["price"])
                return price * amount
                
        except BinanceAPIException as e:
            # If symbol doesn't exist, it's ok - we'll return None
            if e.code == -1121:  # Invalid symbol
                return None
            # For other errors, log but don't fail the entire operation
            logger.warning(f"API error getting USDT value for {coin}: {str(e)}")
            
        except Exception as e:
            logger.warning(f"Error getting USDT value for {coin}: {str(e)}")
            
        return None

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type((ConnectionError, TimeoutError))
    )

    async def get_klines(self, 
                 symbol: str, 
                 interval: str = config.KLINES_INTERVAL, 
                 limit: int = 500, 
                 start: Optional[int] = None, 
                 end: Optional[int] = None
                 ) -> Optional[List[List[Any]]]:
        """Fetch candlestick data with enhanced caching and error handling."""
        await self._ensure_initialized()
        
        # Validate parameters
        if not symbol:
            raise ValueError("Symbol is required")
            
        valid_intervals = [
            "1m", "3m", "5m", "15m", "30m",
            "1h", "2h", "4h", "6h", "8h", "12h",
            "1d", "3d", "1w", "1M"
        ]
        
        if interval not in valid_intervals:
            logger.warning("Invalid kline interval: %s. Using default %s", 
                           interval, config.KLINES_INTERVAL)
            interval = config.KLINES_INTERVAL
        
        if limit <= 0 or limit > 1000:
            logger.warning("Invalid limit: %d. Must be between 1 and 1000", limit)
            limit = min(max(1, limit), 1000)
        
        # Calculate cache key
        cache_key = f"{symbol}_{interval}_{limit}_{start}_{end}"
        
        # Check if we have this in our cache
        if not hasattr(self, '_klines_cache'):
            self._klines_cache = {}
        
        current_time = time.time()
        # Determine appropriate cache duration based on interval
        cache_duration = 60  # Default 60 seconds
        if interval.endswith('m'):
            # For minute intervals, cache for interval duration
            cache_duration = int(interval[:-1]) * 60
        elif interval.endswith('h'):
            # For hour intervals, cache for interval duration
            cache_duration = int(interval[:-1]) * 3600
        elif interval in ['1d', '3d', '1w', '1M']:
            # For day/week/month intervals, cache longer
            cache_duration = 3600  # 1 hour
        
        # Longer historical data can be cached longer
        if limit > 100 or start is not None:
            cache_duration *= 2
        
        if cache_key in self._klines_cache:
            cache_entry = self._klines_cache[cache_key]
            cache_age = current_time - cache_entry.get('timestamp', 0)
            
            if cache_age < cache_duration:
                logger.debug(f"Using cached klines for {cache_key} (age: {cache_age:.1f}s)")
                return cache_entry.get('data')
        
        # If not in cache or cache expired, fetch from API
        try:
            await self._track_request(weight=2)  # Klines endpoint has weight of 2
            klines = await self.client.get_klines(
                symbol=symbol, 
                interval=interval, 
                limit=limit, 
                startTime=start, 
                endTime=end
            )
            
            # Update cache
            self._klines_cache[cache_key] = {
                'data': klines,
                'timestamp': current_time
            }
            
            # Cleanup old cache entries periodically
            if len(self._klines_cache) > 1000:  # If cache gets too large
                self._cleanup_klines_cache()
                
            logger.debug("Fetched %d historical klines for %s", len(klines), symbol)
            return klines
        except BinanceAPIException as e:
            self.last_error_time = time.time()
            logger.error("Binance API error fetching historical klines for %s: [%s] %s", 
                         symbol, e.code, str(e))
            if e.code == -1121:  # Invalid symbol
                raise ValueError(f"Invalid symbol: {symbol}")
            elif e.code == -1021:  # Timestamp out of sync
                raise ConnectionError(f"Time synchronization error: {str(e)}")
            elif e.code == -1003:  # Too many requests
                raise TimeoutError(f"Rate limit exceeded: {str(e)}")
            else:
                raise
        except Exception as e:
            logger.error("Error fetching historical klines for %s: %s", symbol, str(e))
            logger.debug("Get historical klines error traceback: %s", traceback.format_exc())
            # Try to return cached data as fallback even if expired
            if cache_key in self._klines_cache:
                logger.warning(f"Using expired cache as fallback for {cache_key}")
                return self._klines_cache[cache_key].get('data')
            return None

    async def _cleanup_klines_cache(self):
        """Clean up old klines cache entries."""
        current_time = time.time()
        keys_to_remove = []
        
        # Find old entries
        for key, entry in self._klines_cache.items():
            if current_time - entry.get('timestamp', 0) > 3600:  # Older than 1 hour
                keys_to_remove.append(key)
        
        # Remove oldest entries first if cache is too large
        if len(self._klines_cache) > 1000 and len(keys_to_remove) < 200:
            # Sort all entries by age
            sorted_keys = sorted(
                self._klines_cache.keys(),
                key=lambda k: self._klines_cache[k].get('timestamp', 0)
            )
            # Remove oldest 20%
            keys_to_remove = sorted_keys[:200]
        
        # Delete entries
        for key in keys_to_remove:
            del self._klines_cache[key]
        
        logger.debug(f"Cleaned up {len(keys_to_remove)} old klines cache entries")
    
    async def get_exchange_info(self) -> Dict[str, Any]:
        """Retrieve and cache exchange information from Binance.
        
        Returns:
            Dict[str, Any]: Exchange information
        
        Raises:
            ConnectionError: If connection to Binance fails
            TimeoutError: If rate limit is exceeded
            Exception: For other errors
        """
        await self._ensure_initialized()
        
        # Check if cached exchange info is still valid (less than 1 hour old)
        current_time = time.time()
        if hasattr(self, '_exchange_info') and self._exchange_info and \
           hasattr(self, '_exchange_info_timestamp') and \
           (current_time - self._exchange_info_timestamp < 3600):
            logger.debug("Returning cached exchange info")
            return self._exchange_info
        
        # If cache is invalid or doesn't exist, fetch new data
        await self._track_request(weight=10)  # Exchange info has higher weight
        
        try:
            # Add timeout to prevent hanging
            fetch_task = asyncio.create_task(self.client.get_exchange_info())
            info = await asyncio.wait_for(fetch_task, timeout=15.0)  # 15 second timeout
            
            # Cache the result with a timestamp
            self._exchange_info = info
            self._exchange_info_timestamp = current_time
            self._exchange_info_timestamp = time.time()
            logger.debug("Exchange info retrieved and cached successfully")
            return info
            
        except BinanceAPIException as e:
            self.last_error_time = time.time()
            logger.error("Binance API error fetching exchange info: [%s] %s", e.code, str(e))
            
            if e.code == -1021:  # Timestamp out of sync
                raise ConnectionError(f"Time synchronization error: {str(e)}")
            elif e.code == -1003:  # Too many requests
                raise TimeoutError(f"Rate limit exceeded: {str(e)}")
            else:
                raise
                
        except Exception as e:
            logger.error("Error fetching exchange info: %s", str(e))
            logger.debug("Get exchange info error traceback: %s", traceback.format_exc())
            raise

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type((ConnectionError, TimeoutError))
    )

    async def get_ticker_24hr(self, symbol: str = None) -> Union[Dict[str, Any], List[Dict[str, Any]]]:
        """Retrieve 24-hour ticker data from Binance with enhanced error handling.
        
        Args:
            symbol: Optional specific symbol to get ticker for
            
        Returns:
            Dictionary or list of ticker data
            
        Raises:
            ConnectionError: If connection to Binance fails
            Exception: For other errors
        """
        await self._ensure_initialized()
        
        # Weight varies based on request
        weight = 1 if symbol else 40  # All symbols has higher weight
        await self._track_request(weight=weight)
        
        try:
            if symbol:
                ticker = await self.client.get_symbol_ticker(symbol=symbol)
                logger.debug("Ticker for %s retrieved using get_symbol_ticker", symbol)
                return ticker
            else:
                if hasattr(self.client, 'get_ticker_24hr'):
                    tickers = await self.client.get_ticker_24hr()
                    logger.debug("All tickers retrieved using get_ticker_24hr")
                    return tickers
                else:
                    error_msg = "Fetching all tickers 24hr is not supported in the current client version."
                    logger.error(error_msg)
                    raise NotImplementedError(error_msg)
                    
        except BinanceAPIException as e:
            self.last_error_time = time.time()
            logger.error("Binance API error fetching ticker data: [%s] %s", e.code, str(e))
            
            if e.code == -1121:  # Invalid symbol
                raise ValueError(f"Invalid symbol: {symbol}")
            elif e.code == -1021:  # Timestamp out of sync
                raise ConnectionError(f"Time synchronization error: {str(e)}")
            elif e.code == -1003:  # Too many requests
                raise TimeoutError(f"Rate limit exceeded: {str(e)}")
            else:
                raise
                
        except Exception as e:
            logger.error("Error fetching ticker data: %s", str(e))
            logger.debug("Get ticker error traceback: %s", traceback.format_exc())
            raise

    async def create_buy_order(
        self, 
        symbol: str, 
        quantity: float, 
        price: float, 
        stop_loss: Optional[float] = None, 
        take_profit: Optional[float] = None,
        client_order_id: Optional[str] = None
    ) -> Optional[str]:
        """Create a limit buy order with optional stop-loss and take-profit.
        
        Args:
            symbol: The trading pair symbol
            quantity: Order quantity
            price: Order price
            stop_loss: Optional stop loss price
            take_profit: Optional take profit price
            client_order_id: Optional client order ID
            
        Returns:
            Order ID or None if creation failed
            
        Raises:
            ValueError: If parameters are invalid
            ConnectionError: If connection to Binance fails
            Exception: For other errors
        """
        await self._ensure_initialized()
        
        # Validate parameters
        if not symbol:
            raise ValueError("Symbol is required")
            
        if quantity <= 0:
            raise ValueError("Quantity must be positive")
            
        if price <= 0:
            raise ValueError("Price must be positive")
        
        # Prepare order parameters
        try:
            # Adjust quantity precision according to symbol rules
            quantity = await self._adjust_quantity_precision(symbol, quantity)
            price = await self._adjust_price_precision(symbol, price)
            
            # Track API request
            await self._track_request()
            
            # Generate client order ID if not provided
            if not client_order_id:
                client_order_id = f"buy_{int(time.time() * 1000)}_{uuid.uuid4().hex[:8]}"
            
            # Create the main buy order
            order_params = {
                "symbol": symbol,
                "side": "BUY",
                "type": "LIMIT",
                "timeInForce": "GTC",
                "quantity": quantity,
                "price": price,
                "newClientOrderId": client_order_id,
                "newOrderRespType": "FULL"
            }
                
            logger.info("Creating buy order: %s", order_params)
            order = await self.client.create_order(**order_params)
            
            # Extract order ID
            order_id = order.get("orderId")
            if not order_id:
                error_msg = order.get("msg", "Unknown error")
                logger.error("Buy order failed: %s", error_msg)
                raise Exception(f"Buy order failed: {error_msg}")
            
            # Store order info for later reference
            try:
                await self._store_order_info(order, "BUY", client_order_id)
            except Exception as e:
                logger.warning(f"Failed to store order info: {str(e)}")
            
            # Create stop-loss order if specified
            if stop_loss and stop_loss > 0:
                await self._track_request()
                try:
                    stop_loss = await self._adjust_price_precision(symbol, stop_loss)
                    stop_limit_price = await self._adjust_price_precision(symbol, stop_loss * 0.99)
                    
                    stop_loss_params = {
                        "symbol": symbol,
                        "side": "SELL",
                        "type": "STOP_LOSS_LIMIT",
                        "timeInForce": "GTC",
                        "quantity": quantity,
                        "stopPrice": stop_loss,
                        "price": stop_limit_price,
                        "newClientOrderId": f"sl_{client_order_id}",
                        "newOrderRespType": "FULL"
                    }
                    
                    sl_order = await self.client.create_order(**stop_loss_params)
                    sl_order_id = sl_order.get("orderId")
                    
                    if sl_order_id:
                        logger.info("Stop-loss order %s created for buy order %s at %f", 
                                    sl_order_id, order_id, stop_loss)
                        await self._store_order_info(sl_order, "STOP_LOSS", f"sl_{client_order_id}")
                except Exception as e:
                    logger.error("Failed to create stop-loss for buy order %s: %s", order_id, str(e))
            
            # Create take-profit order if specified
            if take_profit and take_profit > 0:
                await self._track_request()
                try:
                    take_profit = await self._adjust_price_precision(symbol, take_profit)
                    
                    take_profit_params = {
                        "symbol": symbol,
                        "side": "SELL",
                        "type": "TAKE_PROFIT_LIMIT",
                        "timeInForce": "GTC",
                        "quantity": quantity,
                        "stopPrice": take_profit,
                        "price": take_profit,
                        "newClientOrderId": f"tp_{client_order_id}",
                        "newOrderRespType": "FULL"
                    }
                    
                    tp_order = await self.client.create_order(**take_profit_params)
                    tp_order_id = tp_order.get("orderId")
                    
                    if tp_order_id:
                        logger.info("Take-profit order %s created for buy order %s at %f", 
                                    tp_order_id, order_id, take_profit)
                        await self._store_order_info(tp_order, "TAKE_PROFIT", f"tp_{client_order_id}")
                except Exception as e:
                    logger.error("Failed to create take-profit for buy order %s: %s", order_id, str(e))
            
            logger.info("Buy order %s placed for %s: qty=%f, price=%f", order_id, symbol, quantity, price)
            return str(order_id)
            
        except BinanceAPIException as e:
            self.last_error_time = time.time()
            error_code = e.code
            error_msg = str(e)
            
            logger.error("Binance API error creating buy order for %s: [%s] %s", 
                        symbol, error_code, error_msg)
            
            if error_code == -1013:  # Filter failure (e.g., insufficient funds)
                raise ValueError(f"Order failed: {error_msg}")
            elif error_code == -1121:  # Invalid symbol
                raise ValueError(f"Invalid symbol: {symbol}")
            elif error_code == -1021:  # Timestamp out of sync
                raise ConnectionError(f"Time synchronization error: {error_msg}")
            elif error_code == -1003:  # Too many requests
                raise TimeoutError(f"Rate limit exceeded: {error_msg}")
            else:
                raise
                
        except Exception as e:
            logger.error("Error creating buy order for %s: %s", symbol, str(e))
            logger.debug("Create buy order error traceback: %s", traceback.format_exc())
            return None

    async def create_sell_order(
        self, 
        symbol: str, 
        quantity: float, 
        price: float, 
        stop_loss: Optional[float] = None, 
        take_profit: Optional[float] = None,
        client_order_id: Optional[str] = None
    ) -> Optional[str]:
        """Create a limit sell order with optional stop-loss and take-profit.
        
        Args:
            symbol: The trading pair symbol
            quantity: Order quantity
            price: Order price
            stop_loss: Optional stop loss price
            take_profit: Optional take profit price
            client_order_id: Optional client order ID
            
        Returns:
            Order ID or None if creation failed
            
        Raises:
            ValueError: If parameters are invalid
            ConnectionError: If connection to Binance fails
            Exception: For other errors
        """
        await self._ensure_initialized()
        
        # Validate parameters
        if not symbol:
            raise ValueError("Symbol is required")
            
        if quantity <= 0:
            raise ValueError("Quantity must be positive")
            
        if price <= 0:
            raise ValueError("Price must be positive")
        
        # Prepare order parameters
        try:
            # Adjust quantity precision according to symbol rules
            quantity = await self._adjust_quantity_precision(symbol, quantity)
            price = await self._adjust_price_precision(symbol, price)
            
            # Track API request
            await self._track_request()
            
            # Generate client order ID if not provided
            if not client_order_id:
                client_order_id = f"sell_{int(time.time() * 1000)}_{uuid.uuid4().hex[:8]}"
            
            # Create the main sell order
            order_params = {
                "symbol": symbol,
                "side": "SELL",
                "type": "LIMIT",
                "timeInForce": "GTC",
                "quantity": quantity,
                "price": price,
                "newClientOrderId": client_order_id,
                "newOrderRespType": "FULL"
            }
                
            logger.info("Creating sell order: %s", order_params)
            order = await self.client.create_order(**order_params)
            
            # Extract order ID
            order_id = order.get("orderId")
            if not order_id:
                error_msg = order.get("msg", "Unknown error")
                logger.error("Sell order failed: %s", error_msg)
                raise Exception(f"Sell order failed: {error_msg}")
            
            # Store order info for later reference
            try:
                await self._store_order_info(order, "SELL", client_order_id)
            except Exception as e:
                logger.warning(f"Failed to store order info: {str(e)}")
            
            # Create stop-loss order if specified (for selling, this is a higher price)
            if stop_loss and stop_loss > 0:
                await self._track_request()
                try:
                    stop_loss = await self._adjust_price_precision(symbol, stop_loss)
                    stop_limit_price = await self._adjust_price_precision(symbol, stop_loss * 1.01)
                    
                    stop_loss_params = {
                        "symbol": symbol,
                        "side": "BUY",  # For sell orders, stop loss is a buy
                        "type": "STOP_LOSS_LIMIT",
                        "timeInForce": "GTC",
                        "quantity": quantity,
                        "stopPrice": stop_loss,
                        "price": stop_limit_price,
                        "newClientOrderId": f"sl_{client_order_id}",
                        "newOrderRespType": "FULL"
                    }
                    
                    sl_order = await self.client.create_order(**stop_loss_params)
                    sl_order_id = sl_order.get("orderId")
                    
                    if sl_order_id:
                        logger.info("Stop-loss order %s created for sell order %s at %f", 
                                    sl_order_id, order_id, stop_loss)
                        await self._store_order_info(sl_order, "STOP_LOSS", f"sl_{client_order_id}")
                except Exception as e:
                    logger.error("Failed to create stop-loss for sell order %s: %s", order_id, str(e))
            
            # Create take-profit order if specified (for selling, this is a lower price)
            if take_profit and take_profit > 0:
                await self._track_request()
                try:
                    take_profit = await self._adjust_price_precision(symbol, take_profit)
                    
                    take_profit_params = {
                        "symbol": symbol,
                        "side": "BUY",  # For sell orders, take profit is a buy
                        "type": "TAKE_PROFIT_LIMIT",
                        "timeInForce": "GTC",
                        "quantity": quantity,
                        "stopPrice": take_profit,
                        "price": take_profit,
                        "newClientOrderId": f"tp_{client_order_id}",
                        "newOrderRespType": "FULL"
                    }
                    
                    tp_order = await self.client.create_order(**take_profit_params)
                    tp_order_id = tp_order.get("orderId")
                    
                    if tp_order_id:
                        logger.info("Take-profit order %s created for sell order %s at %f", 
                                    tp_order_id, order_id, take_profit)
                        await self._store_order_info(tp_order, "TAKE_PROFIT", f"tp_{client_order_id}")
                except Exception as e:
                    logger.error("Failed to create take-profit for sell order %s: %s", order_id, str(e))
            
            logger.info("Sell order %s placed for %s: qty=%f, price=%f", order_id, symbol, quantity, price)
            return str(order_id)
            
        except BinanceAPIException as e:
            self.last_error_time = time.time()
            error_code = e.code
            error_msg = str(e)
            
            logger.error("Binance API error creating sell order for %s: [%s] %s", 
                        symbol, error_code, error_msg)
            
            if error_code == -1013:  # Filter failure (e.g., insufficient funds)
                raise ValueError(f"Order failed: {error_msg}")
            elif error_code == -1121:  # Invalid symbol
                raise ValueError(f"Invalid symbol: {symbol}")
            elif error_code == -1021:  # Timestamp out of sync
                raise ConnectionError(f"Time synchronization error: {error_msg}")
            elif error_code == -1003:  # Too many requests
                raise TimeoutError(f"Rate limit exceeded: {error_msg}")
            else:
                raise
                
        except Exception as e:
            logger.error("Error creating sell order for %s: %s", symbol, str(e))
            logger.debug("Create sell order error traceback: %s", traceback.format_exc())
            return None

    async def _store_order_info(self, order: Dict[str, Any], order_type: str, client_order_id: str) -> None:
        """Store order information for later reference.
        
        Args:
            order: The order data from Binance
            order_type: The type of order (BUY, SELL, STOP_LOSS, TAKE_PROFIT)
            client_order_id: The client order ID
        """
        # This could be expanded with a proper database in production
        # For now, just log key info
        order_id = order.get("orderId")
        symbol = order.get("symbol")
        
        logger.debug(f"Stored {order_type} order {order_id} for {symbol} with client ID {client_order_id}")
        
        # In a real implementation, you might store this in a database
        # For example:
        # await db.store_order({
        #     "order_id": order_id,
        #     "client_order_id": client_order_id,
        #     "symbol": symbol,
        #     "type": order_type,
        #     "quantity": float(order.get("origQty", 0)),
        #     "price": float(order.get("price", 0)),
        #     "status": order.get("status"),
        #     "timestamp": order.get("transactTime", int(time.time() * 1000))
        # })

    async def exit_position(self, symbol: str) -> bool:
        """Sell full balance at market price.
        
        Args:
            symbol: The trading pair symbol
            
        Returns:
            True if exit successful, False otherwise
            
        Raises:
            ValueError: If symbol is invalid
            ConnectionError: If connection to Binance fails
            Exception: For other errors
        """
        await self._ensure_initialized()
        
        # Validate parameters
        if not symbol:
            raise ValueError("Symbol is required")
        
        try:
            # Extract base asset from symbol
            base_asset = symbol.replace("USDT", "")
            if not base_asset:
                raise ValueError(f"Invalid symbol format: {symbol}")
                
            # Get account balances
            await self._track_request(weight=5)
            account = await self.client.get_account()
            
            # Find balance for the base asset
            balance = next((float(b["free"]) for b in account["balances"] if b["asset"] == base_asset), 0)
            
            if balance <= 0:
                logger.info("No balance to exit for %s", base_asset)
                return True
                
            # Adjust quantity precision
            quantity = await self._adjust_quantity_precision(symbol, balance)
            
            # Cancel any existing open orders for this symbol first
            try:
                await self._track_request()
                open_orders = await self.client.get_open_orders(symbol=symbol)
                
                if open_orders:
                    logger.info(f"Cancelling {len(open_orders)} open orders for {symbol}")
                    cancel_tasks = []
                    
                    for order in open_orders:
                        order_id = order.get("orderId")
                        if order_id:
                            cancel_tasks.append(self.cancel_order(symbol, order_id=order_id))
                    
                    if cancel_tasks:
                        await asyncio.gather(*cancel_tasks, return_exceptions=True)
                        # Small delay to ensure orders are cancelled before proceeding
                        await asyncio.sleep(0.5)
            except Exception as e:
                logger.warning(f"Error cancelling open orders: {str(e)}")
            
            # Create market sell order
            await self._track_request()
            client_order_id = f"exit_{int(time.time() * 1000)}_{uuid.uuid4().hex[:8]}"
            
            logger.info("Exiting position for %s with quantity %f", symbol, quantity)
            order = await self.client.order_market_sell(
                symbol=symbol, 
                quantity=quantity,
                newClientOrderId=client_order_id
            )
            
            # Check order success
            order_id = order.get("orderId")
            if order_id:
                # Store order info
                try:
                    await self._store_order_info(order, "EXIT", client_order_id)
                except Exception:
                    pass
                    
                logger.info("Position exited for %s: order %s", symbol, order_id)
                return True
                
            logger.error("Exit failed: %s", order.get("msg", "Unknown error"))
            return False
            
        except BinanceAPIException as e:
            self.last_error_time = time.time()
            error_code = e.code
            error_msg = str(e)
            
            logger.error("Binance API error exiting position for %s: [%s] %s", 
                        symbol, error_code, error_msg)
            
            if error_code == -1013:  # Filter failure (e.g., insufficient funds)
                logger.warning("Insufficient balance to exit position for %s", symbol)
                return False
            elif error_code == -1121:  # Invalid symbol
                raise ValueError(f"Invalid symbol: {symbol}")
            elif error_code == -1021:  # Timestamp out of sync
                raise ConnectionError(f"Time synchronization error: {error_msg}")
            elif error_code == -1003:  # Too many requests
                raise TimeoutError(f"Rate limit exceeded: {error_msg}")
            else:
                raise
                
        except Exception as e:
            logger.error("Error exiting position for %s: %s", symbol, str(e))
            logger.debug("Exit position error traceback: %s", traceback.format_exc())
            return False

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type((ConnectionError, TimeoutError))
    )
    async def get_open_positions(self) -> List[Dict[str, Any]]:
        """Get open orders as positions with enhanced data.
        
        Returns:
            List of open positions
            
        Raises:
            ConnectionError: If connection to Binance fails
            Exception: For other errors
        """
        await self._ensure_initialized()
        await self._track_request(weight=3)
        
        try:
            # Get open orders
            orders = await self.client.get_open_orders()
            
            # Process orders and enhance with current price data
            positions = []
            
            # Group orders by symbol to avoid multiple price lookups
            symbol_orders = {}
            for order in orders:
                symbol = order["symbol"]
                if symbol not in symbol_orders:
                    symbol_orders[symbol] = []
                symbol_orders[symbol].append(order)
            
            # Process orders by symbol
            for symbol, symbol_order_list in symbol_orders.items():
                # Get current price once per symbol
                try:
                    await self._track_request()
                    ticker = await self.client.get_symbol_ticker(symbol=symbol)
                    current_price = float(ticker.get("price", 0))
                except Exception as e:
                    logger.warning(f"Failed to get current price for {symbol}: {str(e)}")
                    current_price = 0
                
                # Find OCO orders (linked stop-loss/take-profit orders)
                oco_orders = {}
                for order in symbol_order_list:
                    client_order_id = order.get("clientOrderId", "")
                    if client_order_id.startswith("sl_") or client_order_id.startswith("tp_"):
                        # Extract the base order ID
                        base_id = client_order_id[3:]  # Remove "sl_" or "tp_" prefix
                        if base_id not in oco_orders:
                            oco_orders[base_id] = {"stop_loss": None, "take_profit": None}
                        
                        if client_order_id.startswith("sl_"):
                            oco_orders[base_id]["stop_loss"] = float(order.get("stopPrice", 0))
                        else:
                            oco_orders[base_id]["take_profit"] = float(order.get("stopPrice", 0))
                
                # Process each order
                for order in symbol_order_list:
                    client_order_id = order.get("clientOrderId", "")
                    
                    # Skip OCO orders, they'll be attached to their parent orders
                    if client_order_id.startswith("sl_") or client_order_id.startswith("tp_"):
                        continue
                    
                    # Extract basic order info
                    order_id = order["orderId"]
                    side = order["side"]
                    quantity = float(order["origQty"])
                    price = float(order["price"])
                    entry_time = order["time"]
                    order_type = order["type"]
                    
                    # Find potential OCO associations
                    stop_loss = None
                    take_profit = None
                    if client_order_id in oco_orders:
                        stop_loss = oco_orders[client_order_id]["stop_loss"]
                        take_profit = oco_orders[client_order_id]["take_profit"]
                    
                    # Calculate profit based on position side
                    profit = 0
                    profit_percent = 0
                    
                    if current_price > 0:
                        if side == "BUY":
                            profit = (current_price - price) * quantity
                            profit_percent = (current_price - price) / price * 100
                        else:  # SELL
                            profit = (price - current_price) * quantity
                            profit_percent = (price - current_price) / current_price * 100
                    
                    # Extract strategy info from client order ID if possible
                    strategy = "Manual"
                    if "_" in client_order_id:
                        parts = client_order_id.split("_")
                        if len(parts) >= 3 and parts[0] in ("buy", "sell"):
                            # Format might be "buy_timestamp_strategyname"
                            strategy_part = "_".join(parts[2:])
                            if strategy_part:
                                strategy = strategy_part
                    
                    # Create position object
                    position = {
                        "id": str(order_id),
                        "coinPair": symbol,
                        "quantity": quantity,
                        "entryPrice": price,
                        "currentPrice": current_price,
                        "profit": profit,
                        "profitPercent": profit_percent,
                        "stopLoss": stop_loss,
                        "takeProfit": take_profit,
                        "orderId": str(order_id),
                        "side": side,
                        "time": entry_time,
                        "entryTime": entry_time,
                        "strategy": strategy,
                        "type": order_type,
                        "status": order.get("status", "NEW")
                    }
                    
                    positions.append(position)
                
            logger.debug("Fetched %d open positions", len(positions))
            return positions
            
        except BinanceAPIException as e:
            self.last_error_time = time.time()
            logger.error("Binance API error getting open positions: [%s] %s", e.code, str(e))
            
            if e.code == -1021:  # Timestamp out of sync
                raise ConnectionError(f"Time synchronization error: {str(e)}")
            elif e.code == -1003:  # Too many requests
                raise TimeoutError(f"Rate limit exceeded: {str(e)}")
            else:
                raise
                
        except Exception as e:
            logger.error("Error getting open positions: %s", str(e))
            logger.debug("Get open positions error traceback: %s", traceback.format_exc())
            return []

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type((ConnectionError, TimeoutError))
    )
    async def get_closed_positions(
        self, 
        symbol: str = None, 
        limit: int = 100, 
        start_time: Optional[int] = None, 
        end_time: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """Get recent trades (closed positions) with enhanced data.
        
        Args:
            symbol: Optional trading pair symbol
            limit: Maximum number of positions to return
            start_time: Start time in milliseconds
            end_time: End time in milliseconds
            
        Returns:
            List of closed positions
            
        Raises:
            ValueError: If parameters are invalid
            ConnectionError: If connection to Binance fails
            Exception: For other errors
        """
        await self._ensure_initialized()
        
        # Validate parameters
        if not symbol:
            raise ValueError("Symbol is required")
            
        if limit <= 0:
            logger.warning("Invalid limit: %d. Using default of 100", limit)
            limit = 100
            
        # Cap limit to prevent excessive API usage
        if limit > 1000:
            logger.warning("Limiting request to maximum of 1000 trades")
            limit = 1000
        
        try:
            # Get account trades
            await self._track_request(weight=5)
            
            trades = await self.client.get_account_trades(
                symbol=symbol,
                limit=limit,
                startTime=start_time,
                endTime=end_time
            )
            
            # Process trades into positions format with proper pairing
            
            # Group trades by order ID for potential pairing
            trade_map = {}
            for trade in trades:
                order_id = trade.get("orderId")
                if order_id not in trade_map:
                    trade_map[order_id] = []
                trade_map[order_id].append(trade)
            
            # List to store the final positions
            positions = []
            
            # Process paired trades (buy + sell)
            processed_ids = set()
            
            # Try to build complete positions from orders history first
            try:
                await self._track_request(weight=10)
                # Get order history for better position analysis
                orders = await self.client.get_all_orders(symbol=symbol, limit=limit)
                
                # Group orders by client order ID
                client_id_orders = {}
                for order in orders:
                    if order.get("status") == "FILLED":
                        client_id = order.get("clientOrderId", "")
                        if client_id and not (client_id.startswith("sl_") or client_id.startswith("tp_")):
                            client_id_orders[client_id] = order
                
                # Find stop loss and take profit associations
                for client_id, order in client_id_orders.items():
                    # Look for associated SL/TP orders
                    sl_id = f"sl_{client_id}"
                    tp_id = f"tp_{client_id}"
                    
                    sl_order = next((o for o in orders if o.get("clientOrderId") == sl_id), None)
                    tp_order = next((o for o in orders if o.get("clientOrderId") == tp_id), None)
                    
                    # If we found SL/TP and they were triggered, create a complete position
                    if (sl_order and sl_order.get("status") == "FILLED") or (tp_order and tp_order.get("status") == "FILLED"):
                        triggered_order = sl_order if sl_order and sl_order.get("status") == "FILLED" else tp_order
                        
                        # Find the trades for both orders
                        main_order_id = order.get("orderId")
                        triggered_order_id = triggered_order.get("orderId")
                        
                        main_trades = trade_map.get(main_order_id, [])
                        triggered_trades = trade_map.get(triggered_order_id, [])
                        
                        if main_trades and triggered_trades:
                            # Mark these as processed so they're not processed again
                            processed_ids.add(main_order_id)
                            processed_ids.add(triggered_order_id)
                            
                            # Calculate aggregate info
                            main_side = order.get("side")
                            main_quantity = sum(float(t.get("qty", 0)) for t in main_trades)
                            main_price = sum(float(t.get("price", 0)) * float(t.get("qty", 0)) for t in main_trades) / main_quantity
                            main_time = min(t.get("time", 0) for t in main_trades)
                            
                            triggered_side = triggered_order.get("side")
                            triggered_quantity = sum(float(t.get("qty", 0)) for t in triggered_trades)
                            triggered_price = sum(float(t.get("price", 0)) * float(t.get("qty", 0)) for t in triggered_trades) / triggered_quantity
                            triggered_time = max(t.get("time", 0) for t in triggered_trades)
                            
                            # Determine entry and exit
                            if main_side == "BUY":
                                entry_price = main_price
                                exit_price = triggered_price
                                entry_time = main_time
                                close_time = triggered_time
                            else:
                                entry_price = triggered_price
                                exit_price = main_price
                                entry_time = triggered_time
                                close_time = main_time
                            
                            # Calculate profit
                            profit = (exit_price - entry_price) * main_quantity
                            profit_percent = (exit_price - entry_price) / entry_price * 100
                            
                            # Determine if SL or TP was triggered
                            exit_type = "STOP_LOSS" if sl_order and sl_order.get("status") == "FILLED" else "TAKE_PROFIT"
                            
                            # Calculate fees
                            total_fees = sum(float(t.get("commission", 0)) for t in main_trades + triggered_trades)
                            fee_asset = main_trades[0].get("commissionAsset", "UNKNOWN") if main_trades else "UNKNOWN"
                            
                            if fee_asset != "USDT":
                                fee_usdt = await self._get_usdt_value(fee_asset, total_fees) or 0
                            else:
                                fee_usdt = total_fees
                            
                            net_profit = profit - fee_usdt
                            
                            # Create position object
                            position = {
                                "id": f"{main_order_id}_{triggered_order_id}",
                                "coinPair": symbol,
                                "quantity": main_quantity,
                                "entryPrice": entry_price,
                                "exitPrice": exit_price,
                                "profit": profit,
                                "profitPercent": profit_percent,
                                "fee": fee_usdt,
                                "netProfit": net_profit,
                                "entryTime": entry_time,
                                "closeTime": close_time,
                                "exitType": exit_type,
                                "entrySide": main_side,
                                "exitSide": triggered_side,
                                "entryOrderId": str(main_order_id),
                                "exitOrderId": str(triggered_order_id),
                                "strategy": client_id.split("_")[2] if len(client_id.split("_")) > 2 else "Manual"
                            }
                            
                            positions.append(position)
                
            except Exception as e:
                logger.warning(f"Error processing order history: {str(e)}")
            
            # Process remaining trades that weren't part of a complete position
            for order_id, order_trades in trade_map.items():
                if order_id in processed_ids:
                    continue
                
                for trade in order_trades:
                    # Skip trades already processed
                    if trade.get("id") in processed_ids:
                        continue
                    
                    # Extract basic trade data
                    symbol = trade["symbol"]
                    quantity = float(trade["qty"])
                    price = float(trade["price"])
                    is_buyer = trade["isBuyer"]
                    fee = float(trade["commission"])
                    fee_asset = trade["commissionAsset"]
                    trade_time = trade["time"]
                    
                    # Convert fee to USDT if needed
                    if fee_asset != "USDT":
                        fee_usdt = await self._get_usdt_value(fee_asset, fee) or 0
                    else:
                        fee_usdt = fee
                    
                    # Calculate gross profit
                    quote_qty = float(trade["quoteQty"])
                    
                    # Create isolated position object
                    position = {
                        "id": str(trade["id"]),
                        "coinPair": symbol,
                        "quantity": quantity,
                        "entryPrice": price if is_buyer else None,
                        "exitPrice": price if not is_buyer else None,
                        "profit": 0,  # Can't calculate for isolated trade
                        "profitPercent": 0,
                        "fee": fee_usdt,
                        "netProfit": -fee_usdt,  # Just the fee for isolated trade
                        "side": "BUY" if is_buyer else "SELL",
                        "time": trade_time,
                        "entryTime": trade_time,
                        "closeTime": trade_time,
                        "orderId": str(trade["orderId"]),
                        "strategy": "Manual",
                        "isSingle": True
                    }
                    
                    positions.append(position)
                    processed_ids.add(trade.get("id"))
            
            # Sort positions by time (most recent first)
            positions.sort(key=lambda x: x.get("closeTime", 0), reverse=True)
            
            # Limit to requested count
            positions = positions[:limit]
            
            logger.debug("Fetched %d closed positions", len(positions))
            return positions
            
        except BinanceAPIException as e:
            self.last_error_time = time.time()
            logger.error("Binance API error getting closed positions: [%s] %s", e.code, str(e))
            
            if e.code == -1121:  # Invalid symbol
                raise ValueError(f"Invalid symbol: {symbol}")
            elif e.code == -1021:  # Timestamp out of sync
                raise ConnectionError(f"Time synchronization error: {str(e)}")
            elif e.code == -1003:  # Too many requests
                raise TimeoutError(f"Rate limit exceeded: {str(e)}")
            else:
                raise
                
        except Exception as e:
            logger.error("Error getting closed positions: %s", str(e))
            logger.debug("Get closed positions error traceback: %s", traceback.format_exc())
            return []

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type((ConnectionError, TimeoutError))
    )
    async def get_order_status(self, symbol: str, order_id: Optional[str] = None, 
                             client_order_id: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """Get the status of an order.
        
        Args:
            symbol: The trading pair symbol
            order_id: The Binance order ID
            client_order_id: The client order ID
            
        Returns:
            Order information or None if not found
            
        Raises:
            ValueError: If parameters are invalid
            ConnectionError: If connection to Binance fails
            Exception: For other errors
        """
        await self._ensure_initialized()
        
        # Validate parameters
        if not symbol:
            raise ValueError("Symbol is required")
            
        if not order_id and not client_order_id:
            raise ValueError("Either order_id or client_order_id is required")
        
        try:
            await self._track_request(weight=2)
            
            if order_id:
                order = await self.client.get_order(symbol=symbol, orderId=order_id)
            else:
                order = await self.client.get_order(symbol=symbol, origClientOrderId=client_order_id)
            
            if order:
                logger.debug("Retrieved order status for %s order %s: %s", 
                           symbol, order_id or client_order_id, order.get("status"))
                return order
                
            logger.warning("Order not found: %s", order_id or client_order_id)
            return None
            
        except BinanceAPIException as e:
            self.last_error_time = time.time()
            logger.error("Binance API error getting order status: [%s] %s", e.code, str(e))
            
            # Handle specific error codes
            if e.code == -2013:  # Order does not exist
                logger.warning("Order not found: %s", order_id or client_order_id)
                return None
            elif e.code == -1021:  # Timestamp out of sync
                raise ConnectionError(f"Time synchronization error: {str(e)}")
            elif e.code == -1003:  # Too many requests
                raise TimeoutError(f"Rate limit exceeded: {str(e)}")
            else:
                raise
                
        except Exception as e:
            logger.error("Error getting order status: %s", str(e))
            logger.debug("Get order status error traceback: %s", traceback.format_exc())
            return None

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type((ConnectionError, TimeoutError))
    )
    async def cancel_order(self, symbol: str, order_id: Optional[str] = None, 
                         client_order_id: Optional[str] = None) -> bool:
        """Cancel an open order.
        
        Args:
            symbol: The trading pair symbol
            order_id: The Binance order ID
            client_order_id: The client order ID
            
        Returns:
            True if cancellation successful, False otherwise
            
        Raises:
            ValueError: If parameters are invalid
            ConnectionError: If connection to Binance fails
            Exception: For other errors
        """
        await self._ensure_initialized()
        
        # Validate parameters
        if not symbol:
            raise ValueError("Symbol is required")
            
        if not order_id and not client_order_id:
            raise ValueError("Either order_id or client_order_id is required")
        
        try:
            await self._track_request()
            
            if order_id:
                result = await self.client.cancel_order(symbol=symbol, orderId=order_id)
            else:
                result = await self.client.cancel_order(symbol=symbol, origClientOrderId=client_order_id)
            
            if result and "orderId" in result:
                logger.info("Cancelled order %s for %s", result["orderId"], symbol)
                return True
                
            logger.warning("Order cancellation failed for %s", order_id or client_order_id)
            return False
            
        except BinanceAPIException as e:
            self.last_error_time = time.time()
            logger.error("Binance API error cancelling order: [%s] %s", e.code, str(e))
            
            # Handle specific error codes
            if e.code == -2011:  # Unknown order
                logger.warning("Order not found for cancellation: %s", order_id or client_order_id)
                return False
            elif e.code == -1021:  # Timestamp out of sync
                raise ConnectionError(f"Time synchronization error: {str(e)}")
            elif e.code == -1003:  # Too many requests
                raise TimeoutError(f"Rate limit exceeded: {str(e)}")
            else:
                raise
                
        except Exception as e:
            logger.error("Error cancelling order: %s", str(e))
            logger.debug("Cancel order error traceback: %s", traceback.format_exc())
            return False

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type((ConnectionError, TimeoutError))
    )
    async def get_account_trades(self, symbol: str, limit: int = 500, 
                               start_time: Optional[int] = None, 
                               end_time: Optional[int] = None) -> List[Dict[str, Any]]:
        """Get account trades history with enhanced error handling.
        
        Args:
            symbol: The trading pair symbol
            limit: Maximum number of trades to return
            start_time: Start time in milliseconds
            end_time: End time in milliseconds
            
        Returns:
            List of trades
            
        Raises:
            ValueError: If parameters are invalid
            ConnectionError: If connection to Binance fails
            Exception: For other errors
        """
        await self._ensure_initialized()
        
        # Validate parameters
        if not symbol:
            raise ValueError("Symbol is required")
            
        if limit <= 0:
            logger.warning("Invalid limit: %d. Using default of 500", limit)
            limit = 500
            
        # Cap limit to prevent excessive API usage
        if limit > 1000:
            logger.warning("Limiting request to maximum of 1000 trades")
            limit = 1000
        
        try:
            await self._track_request(weight=5)
            
            trades = await self.client.get_my_trades(
                symbol=symbol,
                limit=limit,
                startTime=start_time,
                endTime=end_time
            )
            
            logger.debug("Fetched %d trades for %s", len(trades), symbol)
            return trades
            
        except BinanceAPIException as e:
            self.last_error_time = time.time()
            logger.error("Binance API error fetching trades: [%s] %s", e.code, str(e))
            
            if e.code == -1121:  # Invalid symbol
                raise ValueError(f"Invalid symbol: {symbol}")
            elif e.code == -1021:  # Timestamp out of sync
                raise ConnectionError(f"Time synchronization error: {str(e)}")
            elif e.code == -1003:  # Too many requests
                raise TimeoutError(f"Rate limit exceeded: {str(e)}")
            else:
                raise
                
        except Exception as e:
            logger.error("Error fetching trades: %s", str(e))
            logger.debug("Get trades error traceback: %s", traceback.format_exc())
            return []

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type((ConnectionError, TimeoutError))
    )
    async def get_my_trades(self, symbol: str, limit: int = 500, 
                           start_time: Optional[int] = None, 
                           end_time: Optional[int] = None) -> List[Dict[str, Any]]:
        """Alias for get_account_trades.
        
        Args:
            symbol: The trading pair symbol
            limit: Maximum number of trades to return
            start_time: Start time in milliseconds
            end_time: End time in milliseconds
            
        Returns:
            List of trades
        """
        return await self.get_account_trades(symbol, limit, start_time, end_time)

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type((ConnectionError, TimeoutError))
    )
    async def get_order_history(self, symbol: str, limit: int = 500, 
                              start_time: Optional[int] = None, 
                              end_time: Optional[int] = None) -> List[Dict[str, Any]]:
        """Get order history with enhanced error handling.
        
        Args:
            symbol: The trading pair symbol
            limit: Maximum number of orders to return
            start_time: Start time in milliseconds
            end_time: End time in milliseconds
            
        Returns:
            List of orders
            
        Raises:
            ValueError: If parameters are invalid
            ConnectionError: If connection to Binance fails
            Exception: For other errors
        """
        await self._ensure_initialized()
        
        # Validate parameters
        if not symbol:
            raise ValueError("Symbol is required")
            
        if limit <= 0:
            logger.warning("Invalid limit: %d. Using default of 500", limit)
            limit = 500
            
        # Cap limit to prevent excessive API usage
        if limit > 1000:
            logger.warning("Limiting request to maximum of 1000 orders")
            limit = 1000
        
        try:
            await self._track_request(weight=5)
            
            orders = await self.client.get_all_orders(
                symbol=symbol,
                limit=limit,
                startTime=start_time,
                endTime=end_time
            )
            
            logger.debug("Fetched %d orders for %s", len(orders), symbol)
            return orders
            
        except BinanceAPIException as e:
            self.last_error_time = time.time()
            logger.error("Binance API error fetching order history: [%s] %s", e.code, str(e))
            
            if e.code == -1121:  # Invalid symbol
                raise ValueError(f"Invalid symbol: {symbol}")
            elif e.code == -1021:  # Timestamp out of sync
                raise ConnectionError(f"Time synchronization error: {str(e)}")
            elif e.code == -1003:  # Too many requests
                raise TimeoutError(f"Rate limit exceeded: {str(e)}")
            else:
                raise
                
        except Exception as e:
            logger.error("Error fetching order history: %s", str(e))
            logger.debug("Get order history error traceback: %s", traceback.format_exc())
            return []

    async def start_websocket_connection(self, stream_name: str, callback) -> str:
        """Start a WebSocket connection to Binance.
        
        Args:
            stream_name: The stream to subscribe to (e.g., 'btcusdt@kline_1m')
            callback: The callback function to receive messages
            
        Returns:
            Connection ID string
            
        Raises:
            Exception: If connection fails
        """
        from binance import BinanceSocketManager
        
        await self._ensure_initialized()
        
        try:
            # Initialize socket manager if needed
            if not hasattr(self, 'socket_manager'):
                self.socket_manager = BinanceSocketManager(self.client)
            
            # Set connection settings
            socket_base_url = "wss://stream.binance.com:9443" if not self.testnet else "wss://testnet.binance.vision"
            
            # Generate a unique connection ID
            connection_id = f"{stream_name}_{uuid.uuid4().hex[:8]}"
            
            # Start the connection
            if "@" in stream_name:  # Single stream
                socket = await self.socket_manager.start_multiplex_socket([stream_name], callback)
            elif stream_name == "user":  # User data stream
                socket = await self.socket_manager.start_user_socket(callback)
            else:
                socket = await self.socket_manager.start_kline_socket(stream_name, callback)
            
            # Store connection details
            self._ws_connections[connection_id] = {
                "socket": socket,
                "stream_name": stream_name,
                "callback": callback,
                "created_at": time.time(),
                "last_message_at": time.time()
            }
            
            logger.info(f"Started WebSocket connection for {stream_name} with ID {connection_id}")
            return connection_id
            
        except Exception as e:
            logger.error(f"Failed to start WebSocket connection for {stream_name}: {str(e)}")
            logger.debug(f"WebSocket connection error traceback: {traceback.format_exc()}")
            raise

    async def stop_websocket_connection(self, connection_id: str) -> bool:
        """Stop a WebSocket connection.
        
        Args:
            connection_id: The connection ID returned by start_websocket_connection
            
        Returns:
            True if successfully stopped, False otherwise
        """
        if not hasattr(self, 'socket_manager'):
            logger.warning("No socket manager found")
            return False
        
        if connection_id not in self._ws_connections:
            logger.warning(f"No WebSocket connection found with ID {connection_id}")
            return False
        
        try:
            connection = self._ws_connections[connection_id]
            await self.socket_manager.stop_socket(connection["socket"])
            del self._ws_connections[connection_id]
            
            logger.info(f"Stopped WebSocket connection {connection_id} for {connection['stream_name']}")
            return True
            
        except Exception as e:
            logger.error(f"Error stopping WebSocket connection {connection_id}: {str(e)}")
            return False

    async def _close_all_ws_connections(self) -> None:
        """Close all active WebSocket connections."""
        if hasattr(self, 'socket_manager'):
            try:
                connection_ids = list(self._ws_connections.keys())
                
                for connection_id in connection_ids:
                    try:
                        await self.stop_websocket_connection(connection_id)
                    except Exception as e:
                        logger.warning(f"Error closing WebSocket connection {connection_id}: {str(e)}")
                
                # If all individual connections are stopped, stop the socket manager
                await self.socket_manager.close()
                logger.info("All WebSocket connections closed")
                
            except Exception as e:
                logger.error(f"Error closing WebSocket connections: {str(e)}")

    async def subscribe_to_kline_updates(self, symbol: str, interval: str, callback) -> str:
        """Subscribe to kline (candlestick) updates.
        
        Args:
            symbol: The trading pair symbol (e.g., 'BTCUSDT')
            interval: The kline interval (e.g., '1m', '5m', '1h')
            callback: Function to call with updates
            
        Returns:
            Connection ID string
        """
        stream_name = f"{symbol.lower()}@kline_{interval}"
        connection_id = await self.start_websocket_connection(stream_name, callback)
        
        # Register this listener
        self._kline_update_listeners[connection_id] = {
            "symbol": symbol,
            "interval": interval,
            "callback": callback
        }
        
        return connection_id

    async def subscribe_to_account_updates(self, callback) -> str:
        """Subscribe to user account updates.
        
        Args:
            callback: Function to call with updates
            
        Returns:
            Connection ID string
        """
        # Get a listen key
        await self._ensure_initialized()
        try:
            listen_key = await self.client.get_listen_key()
            
            # Start user data stream
            connection_id = await self.start_websocket_connection("user", callback)
            
            # Register this listener
            self._account_update_listeners.add(connection_id)
            
            return connection_id
            
        except Exception as e:
            logger.error(f"Failed to subscribe to account updates: {str(e)}")
            raise

    async def subscribe_to_order_updates(self, callback) -> str:
        """Subscribe to order status updates.
        
        Args:
            callback: Function to call with updates
            
        Returns:
            Connection ID string
        """
        # Order updates are part of user data stream, so reuse that
        return await self.subscribe_to_account_updates(callback)

    async def subscribe_to_trade_updates(self, symbol: str, callback) -> str:
        """Subscribe to real-time trade updates.
        
        Args:
            symbol: The trading pair symbol (e.g., 'BTCUSDT')
            callback: Function to call with updates
            
        Returns:
            Connection ID string
        """
        stream_name = f"{symbol.lower()}@trade"
        connection_id = await self.start_websocket_connection(stream_name, callback)
        
        # Register this listener
        self._trade_pair_listeners[connection_id] = {
            "symbol": symbol,
            "callback": callback
        }
        
        return connection_id

    async def get_all_trading_pairs(self) -> List[Dict[str, Any]]:
        """Get all available trading pairs with additional metadata.
        
        Returns:
            List of trading pairs with metadata
        
        This optimized implementation:
        1. Uses cached data with 5-minute TTL when available
        2. Uses a single batch request to get all ticker prices
        3. Filters USDT pairs before requesting prices
        4. Properly handles timeouts and errors
        """
        await self._ensure_initialized()
        
        # Check cache first to avoid repeated API calls
        current_time = time.time()
        if hasattr(self, '_all_trading_pairs_cache') and self._all_trading_pairs_cache:
            cache_age = current_time - self._all_trading_pairs_cache.get('timestamp', 0)
            # Use cache if it's less than 5 minutes old
            if cache_age < 300:  # 5 minute TTL
                logger.debug(f"Using cached trading pairs data ({cache_age:.1f}s old)")
                return self._all_trading_pairs_cache.get('data', [])
        
        try:
            # Get exchange info - this works with all python-binance versions
            exchange_info = await self.get_exchange_info()
            all_symbols = exchange_info.get("symbols", [])
            
            # Pre-filter for active USDT pairs
            usdt_symbols = [
                s for s in all_symbols 
                if s.get("status") == "TRADING" and s.get("quoteAsset") == "USDT"
            ]
            
            # Prepare trading pairs without prices first
            trading_pairs = []
            symbols_map = {}  # For quick lookup when adding prices
            
            for symbol_info in usdt_symbols:
                symbol = symbol_info.get("symbol")
                pair_info = {
                    "symbol": symbol,
                    "baseAsset": symbol_info.get("baseAsset"),
                    "quoteAsset": symbol_info.get("quoteAsset"),
                    "price": 0.0,  # Default placeholder
                    "status": symbol_info.get("status"),
                    "isSpotTradingAllowed": "SPOT" in symbol_info.get("permissions", []),
                    "filters": symbol_info.get("filters", [])
                }
                trading_pairs.append(pair_info)
                symbols_map[symbol] = len(trading_pairs) - 1  # Store index for quick updates
            
            # Get all prices in a single API call
            try:
                # Use a reasonable timeout for the API call
                with_timeout = asyncio.create_task(self.client.get_all_tickers())
                all_tickers = await asyncio.wait_for(with_timeout, timeout=10.0)
                
                # Update prices for our USDT pairs
                for ticker in all_tickers:
                    symbol = ticker.get("symbol")
                    if symbol in symbols_map:
                        idx = symbols_map[symbol]
                        trading_pairs[idx]["price"] = float(ticker.get("price", 0))
            except asyncio.TimeoutError:
                logger.warning("Timeout getting all tickers, returning pairs with default prices")
            except Exception as e:
                logger.warning(f"Error getting all tickers: {str(e)}, returning pairs with default prices")
            
            # Sort by symbol name
            trading_pairs.sort(key=lambda x: x.get("symbol"))
            
            # Cache the results
            self._all_trading_pairs_cache = {
                'data': trading_pairs,
                'timestamp': current_time
            }
            
            logger.info(f"Retrieved {len(trading_pairs)} trading pairs")
            return trading_pairs
            
        except Exception as e:
            logger.error(f"Error getting trading pairs: {str(e)}")
            # Return cached data if available, even if expired
            if hasattr(self, '_all_trading_pairs_cache') and self._all_trading_pairs_cache:
                logger.warning("Returning expired cached trading pairs data due to error")
                return self._all_trading_pairs_cache.get('data', [])
            return []
            logger.debug(f"Get trading pairs error traceback: {traceback.format_exc()}")
            return []

    async def get_market_depth(self, symbol: str, limit: int = 100) -> Dict[str, Any]:
        """Get order book depth for a symbol.
        
        Args:
            symbol: The trading pair symbol
            limit: Depth limit (5, 10, 20, 50, 100, 500, 1000, 5000)
            
        Returns:
            Order book data
        """
        await self._ensure_initialized()
        
        # Validate parameters
        valid_limits = [5, 10, 20, 50, 100, 500, 1000, 5000]
        if limit not in valid_limits:
            logger.warning(f"Invalid depth limit {limit}, using closest valid value")
            # Find closest valid limit
            limit = min(valid_limits, key=lambda x: abs(x - limit))
        
        try:
            await self._track_request(weight=5)  # Order book has higher weight
            
            depth = await self.client.get_order_book(symbol=symbol, limit=limit)
            
            # Process the data to make it more usable
            bids = [[float(bid[0]), float(bid[1])] for bid in depth.get("bids", [])]
            asks = [[float(ask[0]), float(ask[1])] for ask in depth.get("asks", [])]
            
            # Add some summary statistics
            bid_volume = sum(bid[1] for bid in bids)
            ask_volume = sum(ask[1] for ask in asks)
            
            best_bid = bids[0][0] if bids else 0
            best_ask = asks[0][0] if asks else 0
            spread = best_ask - best_bid if best_bid and best_ask else 0
            spread_percent = (spread / best_bid * 100) if best_bid else 0
            
            result = {
                "symbol": symbol,
                "bids": bids,
                "asks": asks,
                "bidVolume": bid_volume,
                "askVolume": ask_volume,
                "bestBid": best_bid,
                "bestAsk": best_ask,
                "spread": spread,
                "spreadPercent": spread_percent,
                "updateId": depth.get("lastUpdateId"),
                "timestamp": int(time.time() * 1000)
            }
            
            logger.debug(f"Got market depth for {symbol} with {len(bids)} bids and {len(asks)} asks")
            return result
            
        except BinanceAPIException as e:
            logger.error(f"Binance API error getting market depth: [{e.code}] {str(e)}")
            raise
            
        except Exception as e:
            logger.error(f"Error getting market depth: {str(e)}")
            raise

    async def create_market_buy_order(self, symbol: str, quantity: float) -> Optional[str]:
        """Create a market buy order.
        
        Args:
            symbol: The trading pair symbol
            quantity: Order quantity
            
        Returns:
            Order ID or None if creation failed
        """
        await self._ensure_initialized()
        
        try:
            # Adjust quantity precision
            quantity = await self._adjust_quantity_precision(symbol, quantity)
            
            # Generate client order ID
            client_order_id = f"market_buy_{int(time.time() * 1000)}_{uuid.uuid4().hex[:8]}"
            
            # Create market buy order
            await self._track_request()
            order = await self.client.order_market_buy(
                symbol=symbol,
                quantity=quantity,
                newClientOrderId=client_order_id
            )
            
            order_id = order.get("orderId")
            if order_id:
                logger.info(f"Market buy order {order_id} placed for {symbol}: qty={quantity}")
                
                # Store order info
                try:
                    await self._store_order_info(order, "MARKET_BUY", client_order_id)
                except Exception as e:
                    logger.warning(f"Failed to store order info: {str(e)}")
                
                return str(order_id)
            
            logger.error(f"Market buy order failed: {order.get('msg', 'Unknown error')}")
            return None
            
        except BinanceAPIException as e:
            self.last_error_time = time.time()
            error_code = e.code
            error_msg = str(e)
            
            logger.error(f"Binance API error creating market buy order: [{error_code}] {error_msg}")
            
            if error_code == -1013:  # Filter failure (e.g., insufficient funds)
                raise ValueError(f"Order failed: {error_msg}")
            elif error_code == -1121:  # Invalid symbol
                raise ValueError(f"Invalid symbol: {symbol}")
            elif error_code == -1021:  # Timestamp out of sync
                raise ConnectionError(f"Time synchronization error: {error_msg}")
            elif error_code == -1003:  # Too many requests
                raise TimeoutError(f"Rate limit exceeded: {error_msg}")
            else:
                raise
                
        except Exception as e:
            logger.error(f"Error creating market buy order: {str(e)}")
            logger.debug(f"Market buy order error traceback: {traceback.format_exc()}")
            return None

    async def create_market_sell_order(self, symbol: str, quantity: float) -> Optional[str]:
        """Create a market sell order.
        
        Args:
            symbol: The trading pair symbol
            quantity: Order quantity
            
        Returns:
            Order ID or None if creation failed
        """
        await self._ensure_initialized()
        
        try:
            # Adjust quantity precision
            quantity = await self._adjust_quantity_precision(symbol, quantity)
            
            # Generate client order ID
            client_order_id = f"market_sell_{int(time.time() * 1000)}_{uuid.uuid4().hex[:8]}"
            
            # Create market sell order
            await self._track_request()
            order = await self.client.order_market_sell(
                symbol=symbol,
                quantity=quantity,
                newClientOrderId=client_order_id
            )
            
            order_id = order.get("orderId")
            if order_id:
                logger.info(f"Market sell order {order_id} placed for {symbol}: qty={quantity}")
                
                # Store order info
                try:
                    await self._store_order_info(order, "MARKET_SELL", client_order_id)
                except Exception as e:
                    logger.warning(f"Failed to store order info: {str(e)}")
                
                return str(order_id)
            
            logger.error(f"Market sell order failed: {order.get('msg', 'Unknown error')}")
            return None
            
        except BinanceAPIException as e:
            self.last_error_time = time.time()
            error_code = e.code
            error_msg = str(e)
            
            logger.error(f"Binance API error creating market sell order: [{error_code}] {error_msg}")
            
            if error_code == -1013:  # Filter failure (e.g., insufficient funds)
                raise ValueError(f"Order failed: {error_msg}")
            elif error_code == -1121:  # Invalid symbol
                raise ValueError(f"Invalid symbol: {symbol}")
            elif error_code == -1021:  # Timestamp out of sync
                raise ConnectionError(f"Time synchronization error: {error_msg}")
            elif error_code == -1003:  # Too many requests
                raise TimeoutError(f"Rate limit exceeded: {error_msg}")
            else:
                raise
                
        except Exception as e:
            logger.error(f"Error creating market sell order: {str(e)}")
            logger.debug(f"Market sell order error traceback: {traceback.format_exc()}")
            return None

    async def cancel_all_orders(self, symbol: str) -> Tuple[int, int]:
        """Cancel all open orders for a symbol.
        
        Args:
            symbol: The trading pair symbol
            
        Returns:
            Tuple of (successful cancellations, failed cancellations)
        """
        await self._ensure_initialized()
        
        try:
            # Get all open orders
            await self._track_request(weight=3)
            open_orders = await self.client.get_open_orders(symbol=symbol)
            
            if not open_orders:
                logger.info(f"No open orders to cancel for {symbol}")
                return 0, 0
            
            logger.info(f"Cancelling {len(open_orders)} open orders for {symbol}")
            
            # Cancel orders
            success_count = 0
            fail_count = 0
            
            for order in open_orders:
                order_id = order.get("orderId")
                
                if order_id:
                    try:
                        await self._track_request()
                        result = await self.client.cancel_order(symbol=symbol, orderId=order_id)
                        
                        if result and "orderId" in result:
                            success_count += 1
                            logger.debug(f"Cancelled order {order_id} for {symbol}")
                        else:
                            fail_count += 1
                            logger.warning(f"Failed to cancel order {order_id} for {symbol}")
                    except Exception as e:
                        fail_count += 1
                        logger.warning(f"Error cancelling order {order_id} for {symbol}: {str(e)}")
            
            logger.info(f"Cancelled {success_count}/{len(open_orders)} orders for {symbol}")
            return success_count, fail_count
            
        except BinanceAPIException as e:
            self.last_error_time = time.time()
            logger.error(f"Binance API error cancelling orders: [{e.code}] {str(e)}")
            raise
            
        except Exception as e:
            logger.error(f"Error cancelling orders: {str(e)}")
            logger.debug(f"Cancel orders error traceback: {traceback.format_exc()}")
            raise

    async def get_system_status(self) -> Dict[str, Any]:
        """Get Binance system status.
        
        Returns:
            System status information
        """
        await self._ensure_initialized()
        
        try:
            # Get system status (only available on api3 endpoint)
            # Use raw request since this isn't available in python-binance
            api_url = "https://api.binance.com/sapi/v1/system/status"
            
            # Use client's session for the request
            session = self.client.session
            headers = {"X-MBX-APIKEY": self.api_key}
            
            async with session.get(api_url, headers=headers) as response:
                if response.status == 200:
                    result = await response.json()
                    
                    # Format the response
                    status_info = {
                        "status": result.get("status", 0),
                        "msg": result.get("msg", "Unknown"),
                        "timestamp": int(time.time() * 1000),
                        "serverTime": await self._get_server_time()
                    }
                    
                    # Add health check information
                    status_info["clientHealth"] = {
                        "timeSyncDiff": self._last_server_time_diff,
                        "lastRequest": time.time() - self.last_request_time if self.last_request_time > 0 else None,
                        "lastError": time.time() - self.last_error_time if self.last_error_time > 0 else None,
                        "activeConnections": len(self._ws_connections)
                    }
                    
                    return status_info
                else:
                    # If the status endpoint fails, use ping as fallback
                    await self.client.ping()
                    return {
                        "status": 0,  # 0 = normal
                        "msg": "Normal",
                        "timestamp": int(time.time() * 1000),
                        "serverTime": await self._get_server_time(),
                        "clientHealth": {
                            "timeSyncDiff": self._last_server_time_diff,
                            "lastRequest": time.time() - self.last_request_time if self.last_request_time > 0 else None,
                            "lastError": time.time() - self.last_error_time if self.last_error_time > 0 else None,
                            "activeConnections": len(self._ws_connections)
                        }
                    }
            
        except Exception as e:
            logger.error(f"Error getting system status: {str(e)}")
            
            # Return degraded status
            return {
                "status": -1,  # -1 = error
                "msg": f"Error: {str(e)}",
                "timestamp": int(time.time() * 1000),
                "error": True,
                "clientHealth": {
                    "timeSyncDiff": self._last_server_time_diff,
                    "lastRequest": time.time() - self.last_request_time if self.last_request_time > 0 else None,
                    "lastError": time.time() - self.last_error_time if self.last_error_time > 0 else None,
                    "activeConnections": len(self._ws_connections)
                }
            }

    async def _get_server_time(self) -> int:
        """Get server time from Binance.
        
        Returns:
            Server time in milliseconds
        """
        try:
            server_time = await self.client.get_server_time()
            return server_time.get("serverTime", int(time.time() * 1000))
        except Exception:
            return int(time.time() * 1000)

    def get_health_metrics(self) -> Dict[str, Any]:
        """Get client health metrics.
        
        Returns:
            Health metrics dictionary
        """
        current_time = time.time()
        
        return {
            "initialized": self.initialized,
            "testnet": self.testnet,
            "timeSyncDiff": self._last_server_time_diff,
            "lastRequestAge": current_time - self.last_request_time if self.last_request_time > 0 else None,
            "lastErrorAge": current_time - self.last_error_time if self.last_error_time > 0 else None,
            "requestCount": self.request_count,
            "requestWeight": self.request_weight,
            "activeConnections": len(self._ws_connections),
            "timestamp": int(current_time * 1000)
        }

    async def create_oco_order(self, symbol: str, side: str, quantity: float, price: float, 
                             stop_price: float, stop_limit_price: Optional[float] = None,
                             client_order_id: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """Create an OCO (One-Cancels-the-Other) order.
        
        Args:
            symbol: The trading pair symbol
            side: Order side (BUY or SELL)
            quantity: Order quantity
            price: Limit order price
            stop_price: Stop order trigger price
            stop_limit_price: Stop-limit order price (defaults to stop_price)
            client_order_id: Optional client order ID
            
        Returns:
            OCO order information or None if creation failed
        """
        await self._ensure_initialized()
        
        # Validate parameters
        if not symbol or not side or not quantity or not price or not stop_price:
            raise ValueError("Missing required parameters")
            
        if side not in ["BUY", "SELL"]:
            raise ValueError(f"Invalid side: {side}")
            
        if quantity <= 0 or price <= 0 or stop_price <= 0:
            raise ValueError("Quantity, price, and stop_price must be positive")
        
        try:
            # Adjust precision
            quantity = await self._adjust_quantity_precision(symbol, quantity)
            price = await self._adjust_price_precision(symbol, price)
            stop_price = await self._adjust_price_precision(symbol, stop_price)
            
            if stop_limit_price is None:
                stop_limit_price = stop_price
            else:
                stop_limit_price = await self._adjust_price_precision(symbol, stop_limit_price)
            
            # Generate client order ID if not provided
            if not client_order_id:
                client_order_id = f"oco_{int(time.time() * 1000)}_{uuid.uuid4().hex[:8]}"
            
            # Create OCO order
            await self._track_request(weight=2)
            
            # Prepare parameters
            params = {
                "symbol": symbol,
                "side": side,
                "quantity": quantity,
                "price": price,
                "stopPrice": stop_price,
                "stopLimitPrice": stop_limit_price,
                "stopLimitTimeInForce": "GTC",
                "listClientOrderId": client_order_id
            }
            
            logger.info(f"Creating OCO order for {symbol}: {params}")
            result = await self.client.order_oco_sell(**params) if side == "SELL" else await self.client.order_oco_buy(**params)
            
            if result and "orderListId" in result:
                logger.info(f"OCO order created for {symbol} with ID {result['orderListId']}")
                return result
            else:
                logger.error(f"OCO order creation failed: {result.get('msg', 'Unknown error')}")
                return None
            
        except BinanceAPIException as e:
            self.last_error_time = time.time()
            error_code = e.code
            error_msg = str(e)
            
            logger.error(f"Binance API error creating OCO order: [{error_code}] {error_msg}")
            
            if error_code == -1013:  # Filter failure (e.g., insufficient funds)
                raise ValueError(f"Order failed: {error_msg}")
            elif error_code == -1121:  # Invalid symbol
                raise ValueError(f"Invalid symbol: {symbol}")
            elif error_code == -1021:  # Timestamp out of sync
                raise ConnectionError(f"Time synchronization error: {error_msg}")
            elif error_code == -1003:  # Too many requests
                raise TimeoutError(f"Rate limit exceeded: {error_msg}")
            else:
                raise
                
        except Exception as e:
            logger.error(f"Error creating OCO order: {str(e)}")
            logger.debug(f"OCO order error traceback: {traceback.format_exc()}")
            return None