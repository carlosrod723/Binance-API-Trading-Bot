# app/api/market_data.py

import asyncio
import json
import logging
from sanic import Blueprint
from sanic.request import Request
from sanic.response import json as sanic_json
from sanic_ext import openapi
from functools import wraps
import time
import websockets
from websockets.exceptions import ConnectionClosed, ConnectionClosedError
import traceback
from typing import Dict, Any, Optional
from functools import lru_cache
import hashlib

# Add this cache function after the imports and before the blueprints
def timed_lru_cache(seconds: int, maxsize: int = 128):
    """LRU Cache decorator with expiration.
    
    Args:
        seconds: Time-to-live in seconds
        maxsize: Maximum cache size
    """
    def wrapper_cache(func):
        func = lru_cache(maxsize=maxsize)(func)
        func.lifetime = seconds
        func.expiration = time.time() + seconds
        
        @wraps(func)
        def wrapped_func(*args, **kwargs):
            if time.time() > func.expiration:
                func.cache_clear()
                func.expiration = time.time() + func.lifetime
            
            return func(*args, **kwargs)
        
        wrapped_func.cache_info = func.cache_info
        wrapped_func.cache_clear = func.cache_clear
        return wrapped_func
    
    return wrapper_cache

def get_cache_key(*args, **kwargs):
    """Generate a cache key from arbitrary arguments."""
    key = str(args) + str(sorted(kwargs.items()))
    return hashlib.md5(key.encode()).hexdigest()

logger = logging.getLogger(__name__)
marketdata_bp = Blueprint("marketdata", url_prefix="/api/v1/marketdata")

def standard_response(success: bool, data: Optional[Dict[str, Any]] = None, error: Optional[str] = None) -> Dict[str, Any]:
    """Create a standardized response format."""
    response = {"success": success}
    if data is not None:
        response.update(data)
    if error:
        response["error"] = error
    return response

# Add the WebSocketManager class here
class WebSocketManager:
    """Manages WebSocket connections to Binance to enable sharing across clients."""
    
    def __init__(self):
        self.connections = {}  # Maps stream_name to connection details
        self.clients = {}  # Maps stream_name to list of connected clients
        self.tasks = {}  # Maps stream_name to asyncio tasks
    
    async def get_connection(self, stream_name, binance_ws_url):
        """Get an existing connection or create a new one."""
        if stream_name in self.connections and self.connections[stream_name]['active']:
            return self.connections[stream_name]['socket']
        
        # Create new connection
        try:
            socket = await websockets.connect(binance_ws_url)
            self.connections[stream_name] = {
                'socket': socket,
                'active': True,
                'created_at': time.time(),
                'last_message_at': time.time()
            }
            
            # Start message forwarding task
            self.tasks[stream_name] = asyncio.create_task(
                self._forward_messages(stream_name, socket)
            )
            
            return socket
        except Exception as e:
            logger.error(f"Error creating WebSocket connection for {stream_name}: {str(e)}")
            raise
    
    async def _forward_messages(self, stream_name, socket):
        """Forward messages from Binance to all connected clients."""
        try:
            while True:
                try:
                    # Receive message from Binance
                    message = await asyncio.wait_for(socket.recv(), timeout=60)
                    
                    # Update last message time
                    self.connections[stream_name]['last_message_at'] = time.time()
                    
                    # Forward to all connected clients
                    if stream_name in self.clients:
                        dead_clients = []
                        for client in self.clients[stream_name]:
                            try:
                                if not client['ws'].closed:
                                    await client['ws'].send(message)
                                else:
                                    dead_clients.append(client)
                            except Exception:
                                dead_clients.append(client)
                        
                        # Clean up dead clients
                        for client in dead_clients:
                            if client in self.clients[stream_name]:
                                self.clients[stream_name].remove(client)
                        
                        # If no clients left, close connection
                        if not self.clients[stream_name]:
                            await self.close_connection(stream_name)
                            break
                
                except asyncio.TimeoutError:
                    # Send ping to check connection
                    try:
                        await socket.ping()
                    except Exception:
                        logger.warning(f"Binance WebSocket ping failed for {stream_name}")
                        break
                
                except Exception as e:
                    logger.error(f"Error in message forwarding for {stream_name}: {str(e)}")
                    break
            
        except Exception as e:
            logger.error(f"Fatal error in forwarding task for {stream_name}: {str(e)}")
        
        finally:
            # Mark connection as inactive
            if stream_name in self.connections:
                self.connections[stream_name]['active'] = False
            
            # Close socket explicitly
            try:
                if socket and not socket.closed:
                    await socket.close()
            except Exception:
                pass
    
    async def add_client(self, stream_name, ws, client_id):
        """Add a client to a stream."""
        if stream_name not in self.clients:
            self.clients[stream_name] = []
        
        self.clients[stream_name].append({
            'ws': ws,
            'id': client_id,
            'connected_at': time.time()
        })
    
    async def remove_client(self, stream_name, client_id):
        """Remove a client from a stream."""
        if stream_name in self.clients:
            self.clients[stream_name] = [
                client for client in self.clients[stream_name] 
                if client['id'] != client_id
            ]
            
            # If no clients left, close connection
            if not self.clients[stream_name]:
                await self.close_connection(stream_name)
    
    async def close_connection(self, stream_name):
        """Close a WebSocket connection."""
        if stream_name in self.connections:
            try:
                socket = self.connections[stream_name]['socket']
                if socket and not socket.closed:
                    await socket.close()
            except Exception as e:
                logger.error(f"Error closing WebSocket for {stream_name}: {str(e)}")
            
            # Clean up connection data
            self.connections[stream_name]['active'] = False
            
            # Cancel forwarding task
            if stream_name in self.tasks and not self.tasks[stream_name].done():
                self.tasks[stream_name].cancel()
                try:
                    await self.tasks[stream_name]
                except asyncio.CancelledError:
                    pass
                del self.tasks[stream_name]

def validate_symbol(f):
    @wraps(f)
    async def decorated(request, *args, **kwargs):
        symbol = request.args.get("symbol")
        if not symbol:
            return sanic_json(
                standard_response(False, error="Symbol parameter is required"), 
                status=400
            )
        
        # Skip validation for special value 'all' used by dashboard
        if symbol.lower() == 'all':
            # Store a flag indicating this is a special case
            request.ctx.is_all_symbols = True
            return await f(request, *args, **kwargs)
        
        try:
            # Initialize trading pairs cache if needed
            if not hasattr(request.app.ctx, 'valid_trading_pairs'):
                request.app.ctx.valid_trading_pairs = {
                    'pairs': set(),
                    'last_updated': 0
                }
            
            # Check if we need to refresh trading pairs (every 4 hours)
            current_time = time.time()
            cache_age = current_time - request.app.ctx.valid_trading_pairs['last_updated']
            
            # Refresh cache if it's empty or older than 4 hours
            if not request.app.ctx.valid_trading_pairs['pairs'] or cache_age > 14400:
                client = request.app.ctx.binance_client
                if not client:
                    logger.error("Binance client not initialized for symbol validation")
                    # Basic format validation if client unavailable
                    if not symbol.isalnum() or len(symbol) < 5:
                        return sanic_json(
                            standard_response(False, error="Invalid symbol format. Expected format: BTCUSDT"), 
                            status=400
                        )
                else:
                    try:
                        # Fetch all trading pairs from Binance
                        trading_pairs = await client.get_all_trading_pairs()
                        valid_symbols = set(pair.get('symbol') for pair in trading_pairs)
                        
                        # Update cache
                        request.app.ctx.valid_trading_pairs = {
                            'pairs': valid_symbols,
                            'last_updated': current_time
                        }
                        
                        logger.info(f"Updated trading pairs cache with {len(valid_symbols)} symbols")
                    except Exception as e:
                        logger.error(f"Error fetching trading pairs: {str(e)}")
                        # If we can't refresh, but have existing cache, continue using it
                        if not request.app.ctx.valid_trading_pairs['pairs']:
                            # Basic format validation as fallback
                            if not symbol.isalnum() or len(symbol) < 5:
                                return sanic_json(
                                    standard_response(False, error="Invalid symbol format. Expected format: BTCUSDT"),
                                    status=400
                                )
            
            # Now validate the symbol against our cache
            if request.app.ctx.valid_trading_pairs['pairs'] and symbol not in request.app.ctx.valid_trading_pairs['pairs']:
                return sanic_json(
                    standard_response(False, error=f"Unknown trading pair: {symbol}. Please use a valid Binance trading pair."),
                    status=400
                )
                
        except Exception as e:
            logger.error(f"Symbol validation error: {str(e)}")
            # On error, do basic validation at minimum
            if not symbol.isalnum() or len(symbol) < 5:
                return sanic_json(
                    standard_response(False, error="Invalid symbol format. Expected format: BTCUSDT"),
                    status=400
                )
        
        return await f(request, *args, **kwargs)
    return decorated

@marketdata_bp.route("/current", methods=["GET"])
@openapi.tag("Market Data")
@openapi.summary("Get current market data")
@openapi.description("Fetches the latest market data for a specified symbol from Binance via REST API.")
@openapi.parameter("symbol", str, "query", required=True, description="Trading pair symbol, e.g. BTCUSDT")
@openapi.parameter("interval", str, "query", required=False, description="Kline interval (default: 1m). Options: 1m, 3m, 5m, 15m, 30m, 1h, 2h, 4h, 6h, 8h, 12h, 1d, 3d, 1w, 1M")
@openapi.parameter("limit", int, "query", required=False, description="Number of data points to return (default: 100, max: 1000)")
@openapi.response(200, {"application/json": {"success": bool, "marketData": list, "count": int}})
@openapi.response(400, {"application/json": {"success": bool, "error": str}})
@openapi.response(500, {"application/json": {"success": bool, "error": str}})
@openapi.response(503, {"application/json": {"success": bool, "error": str}})
@validate_symbol
async def get_current_market_data(request: Request):
    """Get current market data for a symbol with customizable interval and limit.
    
    This optimized implementation:
    1. Uses more aggressive caching with longer TTLs
    2. Enforces request timeouts to avoid hanging requests
    3. Returns partial/cached data when possible instead of failing
    4. Implements progressive caching based on data age
    """
    request_id = id(request)
    symbol = request.args.get("symbol")
    interval = request.args.get("interval", "1m")
    start_time = time.time()
    
    # Handle special 'all' symbol case differently (used by dashboard)
    if hasattr(request.ctx, 'is_all_symbols') and request.ctx.is_all_symbols:
        return await get_dashboard_summary_data(request)
    
    # Validate interval
    valid_intervals = ["1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "6h", "8h", "12h", "1d", "3d", "1w", "1M"]
    if interval not in valid_intervals:
        logger.warning(f"[{request_id}] Invalid interval: {interval}")
        return sanic_json(
            standard_response(False, error=f"Invalid interval. Valid options: {', '.join(valid_intervals)}"), 
            status=400
        )
    
    # Validate limit
    limit = 100  # Default limit
    limit_str = request.args.get("limit")
    if limit_str:
        try:
            limit = int(limit_str)
            if limit <= 0:
                logger.warning(f"[{request_id}] Invalid limit: {limit}")
                return sanic_json(
                    standard_response(False, error="Limit must be a positive integer"), 
                    status=400
                )
            # Cap maximum limit to prevent performance issues
            limit = min(limit, 1000)
        except ValueError:
            logger.warning(f"[{request_id}] Non-integer limit: {limit_str}")
            return sanic_json(
                standard_response(False, error="Limit must be a valid integer"), 
                status=400
            )
    
    logger.info(f"[{request_id}] Fetching market data for {symbol}, interval={interval}, limit={limit}")
    
    client = request.app.ctx.binance_client
    if not client:
        logger.error(f"[{request_id}] Binance client not initialized")
        return sanic_json(
            standard_response(False, error="Market data service unavailable"), 
            status=503
        )
    
    # Get cached data based on parameters
    cache_key = get_cache_key(symbol, interval, limit)
    
    # Determine cache TTL based on interval - INCREASED CACHE TIMES
    cache_ttl = 30  # Default 30 seconds (increased from 10)
    if interval.endswith('m'):
        # For minute intervals, cache for the interval duration
        mins = int(interval[:-1])
        cache_ttl = max(mins * 60, 30)  # Use full interval as TTL
    elif interval.endswith('h'):
        # For hour intervals, cache for 15 minutes
        cache_ttl = 900  # Increased from 5 minutes to 15 minutes
    elif interval in ['1d', '3d', '1w', '1M']:
        # For day/week/month intervals, cache for 30 minutes
        cache_ttl = 1800  # Increased from 15 minutes to 30 minutes
    
    # Initialize cache if needed
    if not hasattr(request.app.ctx, 'market_data_cache'):
        request.app.ctx.market_data_cache = {}
    
    # Check if we have valid cache
    has_fresh_cache = False
    cached_data = None
    cache_age = float('inf')
    
    if cache_key in request.app.ctx.market_data_cache:
        cache_entry = request.app.ctx.market_data_cache[cache_key]
        cache_age = time.time() - cache_entry['timestamp']
        
        # Check if cache is still valid
        if cache_age < cache_ttl:
            logger.info(f"[{request_id}] Using fresh cached data for {symbol}, age: {cache_age:.1f}s")
            return sanic_json(cache_entry['response'])
        
        # Store cached data for fallback even if expired
        has_fresh_cache = cache_age < cache_ttl * 3  # Consider "fresh enough" if less than 3x TTL
        cached_data = cache_entry['response']
    
    # Cache miss or expired cache - fetch new data with timeout protection
    try:
        # Add rate limiting before making the API call
        if not hasattr(request.app.ctx, 'market_data_last_call'):
            request.app.ctx.market_data_last_call = 0
        
        # Ensure at least 100ms between calls for the same endpoint
        time_since_last = time.time() - request.app.ctx.market_data_last_call
        if time_since_last < 0.1:
            await asyncio.sleep(0.1 - time_since_last)
        
        # Create task with explicit timeout
        fetch_task = asyncio.create_task(client.get_klines(symbol, interval, limit=limit))
        market_data = await asyncio.wait_for(fetch_task, timeout=15.0)  # 15 second timeout
        
        request.app.ctx.market_data_last_call = time.time()
        
        if market_data is None:
            logger.error(f"[{request_id}] Failed to fetch market data for {symbol}")
            
            # Return cached data if available (even if expired) rather than error
            if cached_data:
                logger.warning(f"[{request_id}] Returning expired cached data as fallback for {symbol}")
                return sanic_json(cached_data)
                
            return sanic_json(
                standard_response(False, error="Failed to fetch market data"), 
                status=500
            )
        
        # Process and format the market data for better client consumption
        processed_data = []
        for kline in market_data:
            processed_data.append({
                "openTime": kline[0],
                "open": float(kline[1]),
                "high": float(kline[2]),
                "low": float(kline[3]),
                "close": float(kline[4]),
                "volume": float(kline[5]),
                "closeTime": kline[6],
                "quoteAssetVolume": float(kline[7]),
                "numberOfTrades": int(kline[8]),
                "takerBuyBaseAssetVolume": float(kline[9]),
                "takerBuyQuoteAssetVolume": float(kline[10])
            })
        
        # Prepare response
        response_data = standard_response(True, {
            "marketData": processed_data,
            "count": len(processed_data),
            "symbol": symbol,
            "interval": interval,
            "timestamp": int(time.time() * 1000),
            "cached": False,
            "processingTime": round((time.time() - start_time) * 1000)
        })
        
        # Store in cache
        request.app.ctx.market_data_cache[cache_key] = {
            'response': response_data,
            'timestamp': time.time()
        }
        
        # Limit cache size - memory management
        if len(request.app.ctx.market_data_cache) > 1000:
            # Remove oldest 20% entries
            sorted_keys = sorted(
                request.app.ctx.market_data_cache.keys(),
                key=lambda k: request.app.ctx.market_data_cache[k]['timestamp']
            )
            for old_key in sorted_keys[:200]:
                del request.app.ctx.market_data_cache[old_key]
            
        logger.info(f"[{request_id}] Successfully retrieved {len(processed_data)} klines for {symbol} in {time.time() - start_time:.2f}s")
        return sanic_json(response_data)
    
    except asyncio.TimeoutError:
        logger.error(f"[{request_id}] Timeout fetching market data for {symbol} after {time.time() - start_time:.2f}s")
        
        # Return cached data if available, with a warning flag
        if cached_data:
            # Add warning to cached response
            cached_response = cached_data.copy()
            cached_response["warning"] = "Latest data fetch timed out, showing cached data"
            cached_response["cacheAge"] = round(cache_age)
            return sanic_json(cached_response)
            
        return sanic_json(
            standard_response(False, {
                "symbol": symbol, 
                "interval": interval
            }, error="Data fetch timed out, please try again later"),
            status=503
        )
        
    except ConnectionError as ce:
        logger.error(f"[{request_id}] Connection error fetching market data for {symbol}: {str(ce)}")
        
        # Return cached data if available, with a warning flag
        if cached_data:
            cached_response = cached_data.copy()
            cached_response["warning"] = "Connection error, showing cached data"
            cached_response["cacheAge"] = round(cache_age)
            return sanic_json(cached_response)
            
        return sanic_json(
            standard_response(False, error="Connection error with exchange service"), 
            status=503
        )
        
    except Exception as e:
        logger.error(f"[{request_id}] Error fetching market data for {symbol}: {str(e)}")
        logger.debug(f"[{request_id}] Exception traceback: {traceback.format_exc()}")
        
        # Return cached data if available, with a warning flag
        if cached_data:
            cached_response = cached_data.copy()
            cached_response["warning"] = f"Error: {str(e)}, showing cached data"
            cached_response["cacheAge"] = round(cache_age)
            return sanic_json(cached_response)
            
        return sanic_json(
            standard_response(False, error=f"Failed to fetch market data: {str(e)}"), 
            status=500
        )

# Helper function for dashboard summary data
async def get_dashboard_summary_data(request: Request):
    """Get summary market data for dashboard display."""
    request_id = id(request)
    logger.info(f"[{request_id}] Fetching dashboard summary data")
    
    # Use a longer cache TTL for dashboard data (5 minutes)
    cache_key = "dashboard_summary_data"
    cache_ttl = 300
    
    # Check if we have cached data
    if hasattr(request.app.ctx, 'market_data_cache') and cache_key in request.app.ctx.market_data_cache:
        cache_entry = request.app.ctx.market_data_cache[cache_key]
        cache_age = time.time() - cache_entry['timestamp']
        if cache_age < cache_ttl:
            logger.info(f"[{request_id}] Using cached dashboard data, age: {cache_age:.1f}s")
            return sanic_json(cache_entry['response'])
    
    # No cache or expired, build summary data
    client = request.app.ctx.binance_client
    if not client:
        return sanic_json(
            standard_response(False, error="Market data service unavailable"),
            status=503
        )
    
    try:
        # Get top market pairs by volume (use short timeout)
        top_pairs_task = asyncio.create_task(client.get_all_trading_pairs())
        top_pairs = await asyncio.wait_for(top_pairs_task, timeout=10.0)
        
        # Sort by volume and take top 6 USDT pairs
        top_usdt_pairs = [p for p in top_pairs if p.get("quoteAsset") == "USDT"]
        # We'd ideally sort by volume here, but since we don't have volume in get_all_trading_pairs
        # We'll use the most popular pairs instead
        popular_assets = ["BTC", "ETH", "BNB", "SOL", "XRP", "ADA", "DOGE", "DOT"]
        top_usdt_pairs = sorted(
            top_usdt_pairs,
            key=lambda p: popular_assets.index(p.get("baseAsset")) if p.get("baseAsset") in popular_assets else 999
        )
        top_usdt_pairs = top_usdt_pairs[:6]
        
        # Prepare summary response
        summary_data = {
            "topPairs": top_usdt_pairs,
            "lastUpdated": int(time.time() * 1000)
        }
        
        response_data = standard_response(True, summary_data)
        
        # Store in cache
        if not hasattr(request.app.ctx, 'market_data_cache'):
            request.app.ctx.market_data_cache = {}
            
        request.app.ctx.market_data_cache[cache_key] = {
            'response': response_data,
            'timestamp': time.time()
        }
        
        return sanic_json(response_data)
    
    except Exception as e:
        logger.error(f"[{request_id}] Error fetching dashboard summary: {str(e)}")
        return sanic_json(
            standard_response(False, error=f"Failed to fetch market summary: {str(e)}"),
            status=500
        )

@marketdata_bp.websocket("/ws")
@openapi.tag("Market Data")
@openapi.summary("Stream live market data updates")
@openapi.description("Streams real-time market data updates for a specified symbol via Binance WebSocket. This endpoint proxies Binance's production WebSocket stream and reconnects on errors.")
@openapi.parameter("symbol", str, "query", required=True, description="Trading pair symbol, e.g. BTCUSDT")
@openapi.parameter("interval", str, "query", required=False, description="Kline interval (default: 1m). Options: 1m, 3m, 5m, 15m, 30m, 1h, 2h, 4h, 6h, 8h, 12h, 1d, 3d, 1w, 1M")
async def market_data_websocket(request: Request, ws):
    """WebSocket endpoint for streaming market data with improved error handling and reconnection."""
    request_id = id(request)
    
    # Validate symbol
    symbol = request.args.get("symbol")
    if not symbol:
        await ws.send(json.dumps(standard_response(False, error="Symbol parameter is required")))
        await ws.close(1000, "Missing required parameter")
        return
    
    # Validate interval
    interval = request.args.get("interval", "1m")
    valid_intervals = ["1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "6h", "8h", "12h", "1d", "3d", "1w", "1M"]
    if interval not in valid_intervals:
        await ws.send(json.dumps(
            standard_response(False, error=f"Invalid interval. Valid options: {', '.join(valid_intervals)}")
        ))
        await ws.close(1000, "Invalid parameter")
        return
    
    # Binance WebSocket connection configuration
    binance_ws_url = "wss://stream.binance.com:9443/ws"
    stream_name = f"{symbol.lower()}@kline_{interval}"
    subscription_msg = json.dumps({
        "method": "SUBSCRIBE",
        "params": [stream_name],
        "id": request_id
    })
    
    # Variables for reconnection and client status
    max_retries = 5
    retry_count = 0
    max_delay = 60  # Maximum delay in seconds
    client_status = {"connected": True}  # Shared status to track client connection
    
    # Heartbeat mechanism
    last_message_time = time.time()
    heartbeat_interval = 30  # 30 seconds heartbeat check
    
    logger.info(f"[{request_id}] Starting WebSocket connection for {symbol}, interval={interval}")
    
    async def check_connection():
        """Task to check connection health and send heartbeats."""
        nonlocal last_message_time
        
        while client_status["connected"]:
            current_time = time.time()
            if current_time - last_message_time > heartbeat_interval:
                try:
                    await ws.send(json.dumps({"type": "ping", "timestamp": current_time}))
                    logger.debug(f"[{request_id}] Sent heartbeat for {symbol}")
                except Exception as e:
                    logger.error(f"[{request_id}] Failed to send heartbeat: {str(e)}")
                    client_status["connected"] = False
                    break
            
            await asyncio.sleep(5)  # Check every 5 seconds
    
    # Start the heartbeat check
    heartbeat_task = asyncio.create_task(check_connection())
    
    try:
        while client_status["connected"] and retry_count < max_retries:
            try:
                logger.info(f"[{request_id}] Connecting to Binance WebSocket for {stream_name}")
                
                async with websockets.connect(binance_ws_url) as binance_ws:
                    # Send the subscription message
                    await binance_ws.send(subscription_msg)
                    last_message_time = time.time()
                    
                    # Inform client about successful connection
                    try:
                        await ws.send(json.dumps(
                            standard_response(True, {
                                "message": f"Connected to {stream_name} stream",
                                "timestamp": int(last_message_time * 1000)
                            })
                        ))
                    except Exception as e:
                        logger.error(f"[{request_id}] Error sending connection confirmation: {str(e)}")
                        client_status["connected"] = False
                        break
                    
                    logger.info(f"[{request_id}] Subscribed to {stream_name} on Binance")
                    
                    # Main message relay loop
                    while client_status["connected"]:
                        try:
                            # Set a timeout for message receiving
                            message = await asyncio.wait_for(binance_ws.recv(), timeout=60)
                            last_message_time = time.time()
                            
                            # Forward the message to the client
                            try:
                                await ws.send(message)
                            except Exception as e:
                                logger.error(f"[{request_id}] Error sending to client: {str(e)}")
                                client_status["connected"] = False
                                break
                        except asyncio.TimeoutError:
                            logger.warning(f"[{request_id}] No message received for 60 seconds, sending heartbeat")
                            try:
                                await binance_ws.ping()
                                continue
                            except Exception as e:
                                logger.error(f"[{request_id}] Failed to ping Binance: {str(e)}")
                                break
                        except ConnectionClosedError:
                            logger.warning(f"[{request_id}] Connection to Binance closed")
                            break
                        except Exception as e:
                            logger.error(f"[{request_id}] Error receiving or forwarding message: {str(e)}")
                            break
                    
                    # Reset retry count on successful connection cycle
                    retry_count = 0
                    
            except (ConnectionRefusedError, ConnectionClosedError, ConnectionError) as e:
                # Connection-related errors that might be transient
                if not client_status["connected"]:
                    break  # Client disconnected, stop reconnect attempts
                
                retry_count += 1
                delay = min(2 ** retry_count, max_delay)  # Exponential backoff
                
                logger.warning(f"[{request_id}] Connection error ({retry_count}/{max_retries}). Retry in {delay}s: {str(e)}")
                
                # Notify client about the connection issue
                try:
                    await ws.send(json.dumps(
                        standard_response(False, {
                            "message": f"Connection error. Retrying in {delay} seconds...",
                            "retryCount": retry_count,
                            "maxRetries": max_retries
                        }, error=str(e))
                    ))
                except Exception:
                    logger.error(f"[{request_id}] Failed to send error status to client")
                    client_status["connected"] = False
                    break
                
                # Wait before retry
                await asyncio.sleep(delay)
                
            except Exception as e:
                # Other unexpected errors
                logger.error(f"[{request_id}] Unexpected error in WebSocket: {str(e)}")
                logger.debug(f"[{request_id}] Exception traceback: {traceback.format_exc()}")
                
                if client_status["connected"]:
                    try:
                        await ws.send(json.dumps(
                            standard_response(False, error=f"Unexpected error: {str(e)}")
                        ))
                    except Exception:
                        client_status["connected"] = False
                break
        
        if retry_count >= max_retries and client_status["connected"]:
            logger.error(f"[{request_id}] Maximum retry attempts ({max_retries}) reached")
            try:
                await ws.send(json.dumps(
                    standard_response(False, error=f"Failed to maintain connection after {max_retries} attempts")
                ))
            except Exception:
                client_status["connected"] = False
    
    except Exception as e:
        logger.error(f"[{request_id}] Fatal error in WebSocket handler: {str(e)}")
        logger.debug(f"[{request_id}] Exception traceback: {traceback.format_exc()}")
    
    finally:
        # Clean up resources
        if not heartbeat_task.done():
            heartbeat_task.cancel()
            try:
                await heartbeat_task
            except asyncio.CancelledError:
                pass
        
        logger.info(f"[{request_id}] WebSocket connection for {symbol} closed")
        
        # Ensure WebSocket is closed properly
        try:
            await ws.close(1000, "Connection terminated")
        except Exception:
            pass  # Ignore errors if already closed

@marketdata_bp.listener('before_server_start')
async def setup_websocket_manager(app, loop):
    app.ctx.ws_manager = WebSocketManager()