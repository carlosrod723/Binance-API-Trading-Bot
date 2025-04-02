# app/api/bot.py

from sanic import Blueprint, Sanic, response
from sanic.request import Request
from sanic.response import json
from sanic_ext import openapi
from functools import wraps
import asyncio
import pandas as pd
import numpy as np
import logging
import time
import builtins
import traceback
import json as json_lib
from datetime import datetime, timedelta
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type
from typing import Dict, List, Optional, Any, Tuple, Union, Set
from app.api.strategy import load_strategy, standard_response
from app.config import config
from app.utils.risk_manager import RiskManager

# Add these constants for cache management
CACHE_EXPIRY = {
    'klines': 30,  # 30 seconds for kline data
    'positions': 10,  # 10 seconds for position data
    'account': 60,  # 60 seconds for account data
    'trades': 300,  # 5 minutes for trade history
    'symbols': 86400  # 24 hours for symbol data
}

# Cache storage
CACHE = {
    'klines': {},
    'positions': {
        'data': None,
        'timestamp': 0
    },
    'account': {
        'balance': None,
        'timestamp': 0
    },
    'symbols': {
        'data': None,
        'timestamp': 0
    }
}

async def get_cached_data(client, data_type, key=None, fetch_func=None, *args, **kwargs):
    """Get cached data or fetch new data if cache expired.
    
    Args:
        client: The Binance client
        data_type: Type of data (klines, positions, account, symbols)
        key: Cache key for specific data (e.g., symbol for klines)
        fetch_func: Function to call to fetch new data
        *args, **kwargs: Arguments to pass to fetch_func
    
    Returns:
        Cached or freshly fetched data
    """
    current_time = time.time()
    
    # For specific keyed data like klines
    if key and data_type == 'klines':
        cache_key = f"{key}_{kwargs.get('interval', '1m')}_{kwargs.get('limit', 100)}"
        if (cache_key in CACHE[data_type] and 
                current_time - CACHE[data_type][cache_key]['timestamp'] < CACHE_EXPIRY[data_type]):
            logger.debug(f"Using cached {data_type} for {cache_key}")
            return CACHE[data_type][cache_key]['data']
    # For general data like positions
    elif data_type in CACHE and not key:
        if current_time - CACHE[data_type]['timestamp'] < CACHE_EXPIRY[data_type]:
            logger.debug(f"Using cached {data_type}")
            return CACHE[data_type]['data']
    
    # Cache miss or expired, fetch fresh data
    try:
        # Use rate limiting before API call
        await manage_rate_limits(client)
        
        # Fetch data
        data = await fetch_func(*args, **kwargs)
        
        # Store in cache
        if key and data_type == 'klines':
            CACHE[data_type][cache_key] = {
                'data': data,
                'timestamp': current_time
            }
        elif data_type in CACHE and not key:
            CACHE[data_type]['data'] = data
            CACHE[data_type]['timestamp'] = current_time
        
        return data
    except Exception as e:
        logger.error(f"Error fetching {data_type}: {str(e)}")
        
        # Return expired cache as fallback
        if key and data_type == 'klines' and cache_key in CACHE[data_type]:
            logger.warning(f"Using expired cache for {cache_key} as fallback")
            return CACHE[data_type][cache_key]['data']
        elif data_type in CACHE and not key:
            logger.warning(f"Using expired {data_type} cache as fallback")
            return CACHE[data_type]['data']
        
        return None

# Rate limiting tracker
RATE_LIMIT_STATE = {
    'minute_window': {
        'count': 0,
        'weight': 0,
        'last_reset': 0
    },
    'second_window': {
        'count': 0,
        'last_reset': 0
    }
}

async def manage_rate_limits(client):
    """Manage API rate limits with adaptive throttling."""
    current_time = time.time()
    
    # Reset minute window counter if needed
    if current_time - RATE_LIMIT_STATE['minute_window']['last_reset'] > 60:
        RATE_LIMIT_STATE['minute_window'] = {
            'count': 0,
            'weight': 0,
            'last_reset': current_time
        }
    
    # Reset second window counter if needed
    if current_time - RATE_LIMIT_STATE['second_window']['last_reset'] > 1:
        RATE_LIMIT_STATE['second_window'] = {
            'count': 0,
            'last_reset': current_time
        }
    
    # Increment counters
    RATE_LIMIT_STATE['minute_window']['count'] += 1
    RATE_LIMIT_STATE['minute_window']['weight'] += 1  # Adust as needed
    RATE_LIMIT_STATE['second_window']['count'] += 1
    
    # Apply throttling based on usage
    minute_usage_percent = RATE_LIMIT_STATE['minute_window']['weight'] / 1000  # 1200 weight limit with buffer
    second_usage_percent = RATE_LIMIT_STATE['second_window']['count'] / 15  # 20 requests/second with buffer
    
    # Check client's own request tracking if available
    if hasattr(client, 'request_weight') and client.request_weight > 0:
        client_minute_usage = client.request_weight / 1000
        minute_usage_percent = max(minute_usage_percent, client_minute_usage)
    
    # Determine if throttling is needed
    if second_usage_percent > 0.8:
        # Aggressive throttling for second-based limits
        await asyncio.sleep(0.5)
    elif minute_usage_percent > 0.7:
        # Progressive throttling based on usage
        delay = 0.1 + (minute_usage_percent - 0.7) * 5  # 0.1s at 70%, up to 1.6s at 100%
        logger.debug(f"Rate limit throttling: {delay:.2f}s (usage: {minute_usage_percent:.1%})")
        await asyncio.sleep(delay)

logger = logging.getLogger(__name__)

bot_bp = Blueprint("bot", url_prefix="/api/v1/bot")

# Shared state management
BOT_STATES = {}
PERFORMANCE_METRICS = {}
BOT_WEBSOCKET_CONNECTIONS = set()
BOT_EVENT_HISTORY = []  # Store recent events for monitoring

def validate_api_key(f):
    @wraps(f)
    async def decorated(request: Request, *args, **kwargs):
        api_key = request.headers.get("X-MBX-APIKEY")
        api_secret = request.headers.get("X-MBX-APISECRET")
        if not api_key or not api_secret:
            logger.warning(f"[Request {id(request)}] Missing API key or secret")
            return json(standard_response(False, error="Missing API credentials"), status=401)
        request.ctx.api_key = api_key
        request.ctx.api_secret = api_secret
        return await f(request, *args, **kwargs)
    return decorated

def store_bot_event(event_type: str, details: Dict[str, Any], severity: str = "info", app=None) -> None:
    """Store a bot event in the history log with timestamp."""
    # If app is not provided, try to get it from Sanic.get_app()
    if app is None:
        try:
            from sanic import Sanic
            app = Sanic.get_app()
        except:
            logger.error("Failed to get Sanic app instance for storing bot event")
            return
    
    # Ensure app.ctx.BOT_EVENT_HISTORY exists
    if not hasattr(app.ctx, 'BOT_EVENT_HISTORY'):
        app.ctx.BOT_EVENT_HISTORY = []
    
    event = {
        "timestamp": int(time.time() * 1000),
        "type": event_type,
        "severity": severity,
        "details": details
    }
    
    # Add to history and maintain max size
    app.ctx.BOT_EVENT_HISTORY.append(event)
    if len(app.ctx.BOT_EVENT_HISTORY) > 1000:  # Keep last 1000 events
        app.ctx.BOT_EVENT_HISTORY = app.ctx.BOT_EVENT_HISTORY[-1000:]
    
    # Broadcast to all connected WebSocket clients
    broadcast_to_websockets(event)

def broadcast_to_websockets(data: Dict[str, Any]) -> None:
    """Broadcast data to all connected WebSocket clients with batching."""
    global BOT_WEBSOCKET_CONNECTIONS
    
    if not BOT_WEBSOCKET_CONNECTIONS:
        return
    
    # Convert to JSON once for all clients
    message = json_lib.dumps(data)
    
    # Group into batches to avoid creating too many tasks at once
    connections = list(BOT_WEBSOCKET_CONNECTIONS)
    batch_size = 10
    
    for i in range(0, len(connections), batch_size):
        batch = connections[i:i+batch_size]
        asyncio.create_task(send_batch_ws_messages(batch, message))

async def send_batch_ws_messages(connections, message: str) -> None:
    """Send message to a batch of WebSocket connections."""
    dead_connections = []
    
    for ws in connections:
        try:
            if not ws.closed:
                await ws.send(message)
            else:
                dead_connections.append(ws)
        except Exception as e:
            logger.debug(f"Failed to send message to WebSocket: {str(e)}")
            dead_connections.append(ws)
    
    # Remove dead connections
    if dead_connections:
        for ws in dead_connections:
            if ws in BOT_WEBSOCKET_CONNECTIONS:
                BOT_WEBSOCKET_CONNECTIONS.remove(ws)

async def send_ws_message(ws, message: str) -> None:
    """Send message to WebSocket with error handling."""
    try:
        await ws.send(message)
    except Exception as e:
        logger.warning(f"Failed to send message to WebSocket: {str(e)}")
        # Remove broken connections
        if ws in BOT_WEBSOCKET_CONNECTIONS:
            BOT_WEBSOCKET_CONNECTIONS.remove(ws)

def update_performance_metrics(coin_pair: str, metrics: Dict[str, Any], app=None) -> None:
    """Update performance metrics for a specific coin pair."""
    # If app is not provided, try to get it from Sanic.get_app()
    if app is None:
        try:
            from sanic import Sanic
            app = Sanic.get_app()
        except:
            logger.error("Failed to get Sanic app instance for updating performance metrics")
            return
    
    # Ensure app.ctx.PERFORMANCE_METRICS exists
    if not hasattr(app.ctx, 'PERFORMANCE_METRICS'):
        app.ctx.PERFORMANCE_METRICS = {}
    
    if coin_pair not in app.ctx.PERFORMANCE_METRICS:
        app.ctx.PERFORMANCE_METRICS[coin_pair] = {
            "startTimestamp": int(time.time() * 1000),
            "totalTrades": 0,
            "winningTrades": 0,
            "losingTrades": 0,
            "totalProfit": 0.0,
            "totalFees": 0.0,
            "netProfit": 0.0,
            "highestProfit": 0.0,
            "largestLoss": 0.0,
            "consecutiveWins": 0,
            "consecutiveLosses": 0,
            "winRate": 0.0,
            "averageProfit": 0.0,
            "averageLoss": 0.0,
            "profitFactor": 0.0,
            "lastUpdate": int(time.time() * 1000),
            "recentTrades": []
        }
    
    perf = app.ctx.PERFORMANCE_METRICS[coin_pair]
    
    # Update metrics
    if "tradeCompleted" in metrics and metrics["tradeCompleted"]:
        perf["totalTrades"] += 1
        
        profit = metrics.get("profit", 0.0)
        perf["totalProfit"] += profit
        
        fees = metrics.get("fees", 0.0)
        perf["totalFees"] += fees
        
        net_profit = profit - fees
        perf["netProfit"] += net_profit
        
        # Track win/loss metrics
        if net_profit > 0:
            perf["winningTrades"] += 1
            perf["consecutiveWins"] += 1
            perf["consecutiveLosses"] = 0
            if net_profit > perf["highestProfit"]:
                perf["highestProfit"] = net_profit
        else:
            perf["losingTrades"] += 1
            perf["consecutiveLosses"] += 1
            perf["consecutiveWins"] = 0
            if net_profit < perf["largestLoss"]:
                perf["largestLoss"] = net_profit
        
        # Calculate additional metrics
        if perf["totalTrades"] > 0:
            perf["winRate"] = (perf["winningTrades"] / perf["totalTrades"]) * 100
        
        if perf["winningTrades"] > 0:
            perf["averageProfit"] = perf["totalProfit"] / perf["winningTrades"]
        
        if perf["losingTrades"] > 0:
            perf["averageLoss"] = (perf["totalProfit"] - perf["totalFees"]) / perf["losingTrades"]
        
        if perf["averageLoss"] != 0:
            perf["profitFactor"] = abs(perf["averageProfit"] / perf["averageLoss"]) if perf["averageLoss"] else 0
        
        # Add to recent trades
        trade_info = {
            "timestamp": int(time.time() * 1000),
            "profit": profit,
            "fees": fees,
            "netProfit": net_profit,
            "entryPrice": metrics.get("entryPrice", 0.0),
            "exitPrice": metrics.get("exitPrice", 0.0),
            "quantity": metrics.get("quantity", 0.0),
            "side": metrics.get("side", "UNKNOWN"),
            "strategy": metrics.get("strategy", "Unknown"),
            "exitReason": metrics.get("exitReason", "Unknown")
        }
        
        perf["recentTrades"].append(trade_info)
        
        # Keep only recent trades (max 50)
        if len(perf["recentTrades"]) > 50:
            perf["recentTrades"] = perf["recentTrades"][-50:]
    
    # Update timestamp
    perf["lastUpdate"] = int(time.time() * 1000)

@bot_bp.route("/start", methods=["POST"])
@openapi.tag("Bot Control")
@openapi.summary("Start the trading bot")
@openapi.description("Starts the trading bot with the specified strategy and coin pair on Binance with advanced risk management.")
@openapi.parameter("X-MBX-APIKEY", str, "header", required=True)
@openapi.parameter("X-MBX-APISECRET", str, "header", required=True)
@openapi.body({
    "application/json": {
        "strategyName": str, 
        "coinPair": str, 
        "riskPercent": float, 
        "useTrailingStop": bool,
        "maxOpenPositions": int,
        "enableAutomation": bool
    }
}, required=True)
@openapi.response(200, {"application/json": {"success": bool, "message": str, "botId": str}})
@openapi.response(400, {"application/json": {"success": bool, "error": str}})
@openapi.response(401, {"application/json": {"success": bool, "error": str}})
@openapi.response(500, {"application/json": {"success": bool, "error": str}})
@validate_api_key
async def start_trading(request: Request):
    """Start the trading bot with enhanced risk management."""
    app = request.app  # Access the Sanic app instance
    request_id = builtins.id(request)
    logger.info(f"[{request_id}] Processing start trading request")
    
    try:
        data = request.json or {}
        
        # Required parameters
        strategy_name = data.get("strategyName")
        coin_pair = data.get("coinPair")
        
        if not all([strategy_name, coin_pair]):
            logger.warning(f"[{request_id}] Missing strategyName or coinPair")
            return json(
                standard_response(False, error="Missing strategyName or coinPair"), 
                status=400
            )
        
        # Optional parameters with defaults
        risk_percent = float(data.get("riskPercent", config.MAX_RISK_PERCENT_PER_TRADE))
        use_trailing_stop = bool(data.get("useTrailingStop", True))
        max_open_positions = int(data.get("maxOpenPositions", config.MAX_OPEN_POSITIONS))
        enable_automation = bool(data.get("enableAutomation", True))
            
        # Validate risk percent
        if risk_percent <= 0 or risk_percent > 5:
            logger.warning(f"[{request_id}] Invalid risk percent: {risk_percent}")
            return json(
                standard_response(False, error="Risk percent must be between 0.1 and 5"), 
                status=400
            )
        
        # Check if trading bot is already running for this coin pair
        if getattr(request.app.ctx, "trading_active", False) and getattr(request.app.ctx, "current_coin_pair", None) == coin_pair:
            logger.warning(f"[{request_id}] Trading bot already running for {coin_pair}")
            return json(
                standard_response(False, error=f"Trading bot is already running for {coin_pair}"), 
                status=400
            )
        
        # Check for available slot
        running_bots = sum(1 for v in app.ctx.BOT_STATES.values() if v.get("active", False))
        if running_bots >= max_open_positions:
            logger.warning(f"[{request_id}] Maximum number of bots ({max_open_positions}) already running")
            return json(
                standard_response(False, error=f"Maximum number of bots ({max_open_positions}) already running"), 
                status=400
            )
        
        # Load strategy
        strategy = await load_strategy(request.app, strategy_name)
        if not strategy:
            logger.error(f"[{request_id}] Failed to load strategy: {strategy_name}")
            return json(
                standard_response(False, error=f"Could not load strategy {strategy_name}"), 
                status=500
            )
        
        # Generate a unique bot ID
        bot_id = f"bot_{coin_pair}_{int(time.time())}"
        
        # Initialize bot state in both app.ctx.BOT_STATES and global BOT_STATES
        bot_state = {
            "id": bot_id,
            "coinPair": coin_pair,
            "strategyName": strategy_name,
            "startTime": int(time.time() * 1000),
            "riskPercent": risk_percent,
            "useTrailingStop": use_trailing_stop,
            "active": True,   # CRUCIAL: Bot must be marked active here
            "status": "Starting",
            "lastUpdate": int(time.time() * 1000),
            "currentPosition": None,
            "lastSignal": None,
            "enableAutomation": enable_automation,
            "error": None
        }
        
        # Explicitly ensure app.ctx.BOT_STATES exists
        if not hasattr(app.ctx, 'BOT_STATES'):
            app.ctx.BOT_STATES = {}
            
        # Explicitly ensure global BOT_STATES exists
        global BOT_STATES
        if BOT_STATES is None:
            BOT_STATES = {}
        
        # Set in app context with a distinct copy
        app.ctx.BOT_STATES[bot_id] = bot_state.copy()
        
        # Set in global state with a distinct copy
        BOT_STATES[bot_id] = bot_state.copy()
        
        # Double check that both have active=True
        app.ctx.BOT_STATES[bot_id]["active"] = True
        BOT_STATES[bot_id]["active"] = True
        
        # Log both states for verification
        logger.info(f"[{request_id}] Initialized bot state in app.ctx: {json_lib.dumps(app.ctx.BOT_STATES[bot_id])}")
        logger.info(f"[{request_id}] Initialized bot state in global: {json_lib.dumps(BOT_STATES[bot_id])}")
        logger.info(f"[{request_id}] Bot active state in app.ctx: {app.ctx.BOT_STATES[bot_id].get('active')}")
        logger.info(f"[{request_id}] Bot active state in global: {BOT_STATES[bot_id].get('active')}")
        
        # Update app context
        request.app.ctx.trading_active = True
        request.app.ctx.current_coin_pair = coin_pair
        request.app.ctx.active_strategy = strategy
        request.app.ctx.active_bot_id = bot_id
        
        # Start the trading loop, passing the app instance
        request.app.ctx.trading_task = asyncio.create_task(
            trading_loop(
                app,  # Pass the app instance
                bot_id, 
                coin_pair, 
                strategy, 
                risk_percent, 
                use_trailing_stop,
                enable_automation
            )
        )
        
        # Log bot start event
        store_bot_event(
            "bot_started", 
            {
                "botId": bot_id,
                "coinPair": coin_pair,
                "strategyName": strategy_name,
                "riskPercent": risk_percent,
                "useTrailingStop": use_trailing_stop
            }
        )
        
        logger.info(f"[{request_id}] Trading bot started for {coin_pair} with strategy {strategy_name}")
        return json(
            standard_response(True, {
                "message": "Trading bot started",
                "botId": bot_id,
                "coinPair": coin_pair,
                "strategyName": strategy_name
            })
        )
        
    except Exception as e:
        logger.error(f"[{request_id}] Error starting trading bot: {str(e)}")
        logger.debug(f"[{request_id}] Exception traceback: {traceback.format_exc()}")
        return json(
            standard_response(False, error=f"Server error: {str(e)}"), 
            status=500
        )

@bot_bp.route("/stop", methods=["POST"])
@openapi.tag("Bot Control")
@openapi.summary("Stop the trading bot")
@openapi.description("Stops the currently running trading bot on Binance with orderly shutdown.")
@openapi.parameter("X-MBX-APIKEY", str, "header", required=True)
@openapi.parameter("X-MBX-APISECRET", str, "header", required=True)
@openapi.body({"application/json": {"botId": str, "exitPositions": bool}}, required=True)
@openapi.response(200, {"application/json": {"success": bool, "message": str}})
@openapi.response(400, {"application/json": {"success": bool, "error": str}})
@openapi.response(401, {"application/json": {"success": bool, "error": str}})
@openapi.response(404, {"application/json": {"success": bool, "error": str}})
@validate_api_key
async def stop_trading(request: Request):
    """Stop the trading bot with clean shutdown."""
    app = request.app  # Access the Sanic app instance
    request_id = builtins.id(request)
    logger.info(f"[{request_id}] Processing stop trading request")
    
    try:
        data = request.json or {}
        bot_id = data.get("botId")
        exit_positions = bool(data.get("exitPositions", False))
        
        if not bot_id:
            # If no bot ID provided, try to get the active bot
            if hasattr(request.app.ctx, "active_bot_id"):
                bot_id = request.app.ctx.active_bot_id
            else:
                logger.warning(f"[{request_id}] No botId provided and no active bot found")
                return json(
                    standard_response(False, error="No botId provided and no active bot running"), 
                    status=400
                )
        
        # Ensure both app.ctx.BOT_STATES and global BOT_STATES are initialized
        if not hasattr(app.ctx, 'BOT_STATES'):
            app.ctx.BOT_STATES = {}
            
        global BOT_STATES
        if BOT_STATES is None:
            BOT_STATES = {}
            
        # Sync from global to app context if needed
        if bot_id not in app.ctx.BOT_STATES and bot_id in BOT_STATES:
            app.ctx.BOT_STATES[bot_id] = BOT_STATES[bot_id].copy()
            logger.info(f"[{request_id}] Synced bot {bot_id} from global state to app context")
        
        # Sync from app context to global if needed
        if bot_id not in BOT_STATES and bot_id in app.ctx.BOT_STATES:
            BOT_STATES[bot_id] = app.ctx.BOT_STATES[bot_id].copy()
            logger.info(f"[{request_id}] Synced bot {bot_id} from app context to global state")
        
        # Check if bot exists in either app.ctx.BOT_STATES or global BOT_STATES
        if bot_id not in app.ctx.BOT_STATES and bot_id not in BOT_STATES:
            logger.warning(f"[{request_id}] Bot {bot_id} not found in any state storage")
            return json(
                standard_response(False, error=f"Bot {bot_id} not found"), 
                status=404
            )
        
        # Get combined active state (active if either is active)
        app_ctx_active = app.ctx.BOT_STATES.get(bot_id, {}).get("active", False)
        global_active = BOT_STATES.get(bot_id, {}).get("active", False)
        is_active = app_ctx_active or global_active
        
        # Check if bot is running in either context
        if not is_active:
            logger.warning(f"[{request_id}] Bot {bot_id} is not running (app ctx: {app_ctx_active}, global: {global_active})")
            return json(
                standard_response(False, error=f"Bot {bot_id} is not running"), 
                status=400
            )
        
        # Critical section: Update bot state in BOTH app context and global
        # Use a consistent update time for both
        update_time = int(time.time() * 1000)
        
        # Update in app context if it exists there
        if bot_id in app.ctx.BOT_STATES:
            app.ctx.BOT_STATES[bot_id]["active"] = False
            app.ctx.BOT_STATES[bot_id]["status"] = "Stopping"
            app.ctx.BOT_STATES[bot_id]["lastUpdate"] = update_time
            logger.info(f"[{request_id}] Updated bot {bot_id} state in app context: active=False, status=Stopping")
        
        # Update in global state if it exists there
        if bot_id in BOT_STATES:
            BOT_STATES[bot_id]["active"] = False
            BOT_STATES[bot_id]["status"] = "Stopping"
            BOT_STATES[bot_id]["lastUpdate"] = update_time
            logger.info(f"[{request_id}] Updated bot {bot_id} state in global state: active=False, status=Stopping")
        
        # Perform exit if requested
        if exit_positions:
            # Determine coin pair (prefer app context, fall back to global)
            coin_pair = None
            if bot_id in app.ctx.BOT_STATES:
                coin_pair = app.ctx.BOT_STATES[bot_id].get("coinPair")
            elif bot_id in BOT_STATES:
                coin_pair = BOT_STATES[bot_id].get("coinPair")
                
            if coin_pair:
                try:
                    # Get client
                    client = request.app.ctx.binance_client
                    
                    # Exit position
                    await client.exit_position(coin_pair)
                    logger.info(f"[{request_id}] Exited position for {coin_pair} during bot shutdown")
                    
                    # Update bot state in both places
                    for storage in [app.ctx.BOT_STATES, BOT_STATES]:
                        if bot_id in storage:
                            storage[bot_id]["status"] = "StoppedAndExited"
                            storage[bot_id]["currentPosition"] = None
                    
                except Exception as e:
                    logger.error(f"[{request_id}] Error exiting position: {str(e)}")
                    # Continue with shutdown despite error
        
        # Cancel trading task if it exists
        if hasattr(request.app.ctx, "trading_task") and request.app.ctx.trading_task:
            if not request.app.ctx.trading_task.done() and not request.app.ctx.trading_task.cancelled():
                request.app.ctx.trading_task.cancel()
                try:
                    await request.app.ctx.trading_task
                except asyncio.CancelledError:
                    logger.info(f"[{request_id}] Trading task cancelled")
                except Exception as e:
                    logger.error(f"[{request_id}] Error while waiting for trading task cancellation: {str(e)}")
        
        # Update app context flags
        request.app.ctx.trading_active = False
        request.app.ctx.current_coin_pair = None
        request.app.ctx.active_strategy = None
        request.app.ctx.trading_task = None
        
        # Only clear active_bot_id if it matches the bot we're stopping
        if getattr(request.app.ctx, "active_bot_id", None) == bot_id:
            request.app.ctx.active_bot_id = None
            logger.info(f"[{request_id}] Cleared active_bot_id={bot_id} from app context")
        
        # Special verification: Double check both storages have active=False
        for storage_name, storage in [("app.ctx", app.ctx.BOT_STATES), ("global", BOT_STATES)]:
            if bot_id in storage and storage[bot_id].get("active", False):
                logger.error(f"[{request_id}] Bot {bot_id} still marked as active in {storage_name}! Forcing to false")
                storage[bot_id]["active"] = False
        
        # Special broadcast: Send immediate WebSocket update to all clients
        try:
            for storage in [app.ctx.BOT_STATES, BOT_STATES]:
                if bot_id in storage:
                    # Ensure the bot is marked as inactive with clear status
                    final_state = storage[bot_id].copy()
                    final_state["active"] = False
                    final_state["status"] = "Stopped"
                    
                    # Broadcast this state to all connected WebSocket clients
                    bot_state_message = {
                        "type": "bot_state",
                        "botId": bot_id,
                        "state": final_state,
                        "timestamp": int(time.time() * 1000),
                        "forced": True  # Special flag to indicate forced update
                    }
                    
                    # Use existing broadcast mechanism
                    broadcast_to_websockets(bot_state_message)
                    logger.info(f"[{request_id}] Broadcast forced bot state update for {bot_id}")
                    break  # Only need to do this once with either storage
        except Exception as ws_error:
            logger.error(f"[{request_id}] Error broadcasting bot state update: {str(ws_error)}")
        
        # Log event
        store_bot_event(
            "bot_stopped", 
            {
                "botId": bot_id,
                "exitPositions": exit_positions,
                "reason": "Manual stop",
                "coinPair": app.ctx.BOT_STATES.get(bot_id, {}).get("coinPair") or
                           BOT_STATES.get(bot_id, {}).get("coinPair")
            }
        )
        
        logger.info(f"[{request_id}] Trading bot {bot_id} stopped successfully")
        return json(
            standard_response(True, {
                "message": "Trading bot stopped",
                "botId": bot_id,
                "exitedPositions": exit_positions
            })
        )
            
    except Exception as e:
        logger.error(f"[{request_id}] Error stopping trading bot: {str(e)}")
        logger.debug(f"[{request_id}] Exception traceback: {traceback.format_exc()}")
        return json(
            standard_response(False, error=f"Server error: {str(e)}"), 
            status=500
        )

@bot_bp.route("/status", methods=["GET"])
@openapi.tag("Bot Control")
@openapi.summary("Get trading bot status")
@openapi.description("Get status information about all running trading bots.")
@openapi.parameter("X-MBX-APIKEY", str, "header", required=True)
@openapi.parameter("X-MBX-APISECRET", str, "header", required=True)
@openapi.parameter("botId", str, "query", required=False, description="Optional bot ID to get status for a specific bot")
@openapi.response(200, {"application/json": {"success": bool, "bots": list, "count": int}})
@openapi.response(400, {"application/json": {"success": bool, "error": str}})
@openapi.response(401, {"application/json": {"success": bool, "error": str}})
@openapi.response(404, {"application/json": {"success": bool, "error": str}})
@validate_api_key
async def get_bot_status(request: Request):
    """Get status of all running trading bots or a specific bot."""
    app = request.app  # Access the Sanic app instance
    request_id = builtins.id(request)
    logger.info(f"[{request_id}] Processing bot status request")
    
    try:
        client = request.app.ctx.binance_client
        
        # Check if specific bot ID requested
        bot_id = request.args.get("botId")
        
        # Get positions with caching to avoid redundant API calls
        positions = await get_cached_data(
            client,
            'positions',
            fetch_func=client.get_open_positions
        )
        
        # Ensure app.ctx.BOT_STATES exists (and use module global as fallback)
        global BOT_STATES
        
        # Initialize app.ctx.BOT_STATES if it doesn't exist
        if not hasattr(app.ctx, 'BOT_STATES'):
            app.ctx.BOT_STATES = {}
        
        # First ensure module global BOT_STATES exists
        if BOT_STATES is None:
            BOT_STATES = {}
        
        # Merge states from module global into app context (prioritizing existing app context entries)
        for id, state in BOT_STATES.items():
            if id not in app.ctx.BOT_STATES:
                app.ctx.BOT_STATES[id] = state.copy()
                logger.info(f"[{request_id}] Imported bot {id} from module global to app context")
            elif state.get("active", False) and not app.ctx.BOT_STATES[id].get("active", False):
                # Bot is active in global but not in app context - update active state
                app.ctx.BOT_STATES[id]["active"] = True
                app.ctx.BOT_STATES[id]["status"] = state.get("status", "Running")
                logger.info(f"[{request_id}] Updated bot {id} active state from module global")
        
        # Also update module global with any bots from app context it doesn't have
        for id, state in app.ctx.BOT_STATES.items():
            if id not in BOT_STATES:
                BOT_STATES[id] = state.copy()
                logger.info(f"[{request_id}] Exported bot {id} from app context to module global")
            elif app.ctx.BOT_STATES[id].get("active", False) and not BOT_STATES[id].get("active", False):
                # Bot is active in app context but not in global - update active state
                BOT_STATES[id]["active"] = True
                BOT_STATES[id]["status"] = app.ctx.BOT_STATES[id].get("status", "Running")
                logger.info(f"[{request_id}] Updated bot {id} active state in module global")
        
        # Verify trading loop tasks and update states if needed
        if hasattr(app.ctx, "trading_task") and app.ctx.trading_task and not app.ctx.trading_task.done():
            # We have an active trading task but need to make sure the bot state reflects this
            active_bot_id = getattr(app.ctx, "active_bot_id", None)
            if active_bot_id:
                # Make sure the bot state is marked as active in both places
                if active_bot_id in app.ctx.BOT_STATES:
                    app.ctx.BOT_STATES[active_bot_id]["active"] = True
                    app.ctx.BOT_STATES[active_bot_id]["status"] = "Running"
                    logger.info(f"[{request_id}] Corrected bot {active_bot_id} state to active based on active trading task")
                
                if active_bot_id in BOT_STATES:
                    BOT_STATES[active_bot_id]["active"] = True
                    BOT_STATES[active_bot_id]["status"] = "Running"
        
        # DEBUG: Log both states to help debugging
        logger.info(f"[{request_id}] BOT_STATES in module global ({len(BOT_STATES)}): {list(BOT_STATES.keys())}")
        logger.info(f"[{request_id}] BOT_STATES in app.ctx ({len(app.ctx.BOT_STATES)}): {list(app.ctx.BOT_STATES.keys())}")
        
        # Get active bots from both sources
        module_active_bots = [id for id, state in BOT_STATES.items() if state.get("active", False)]
        ctx_active_bots = [id for id, state in app.ctx.BOT_STATES.items() if state.get("active", False)]
        logger.info(f"[{request_id}] Active bots in module global: {module_active_bots}")
        logger.info(f"[{request_id}] Active bots in app.ctx: {ctx_active_bots}")
        
        if bot_id:
            # Get specific bot status - search in both places
            if bot_id in app.ctx.BOT_STATES:
                bot_status = app.ctx.BOT_STATES[bot_id].copy()
            elif bot_id in BOT_STATES:
                bot_status = BOT_STATES[bot_id].copy()
                # Add to app context for future
                app.ctx.BOT_STATES[bot_id] = bot_status.copy()
            else:
                logger.warning(f"[{request_id}] Bot {bot_id} not found in either storage")
                return json(
                    standard_response(False, error=f"Bot {bot_id} not found"), 
                    status=404
                )
            
            # Update with current position info if available
            coin_pair = bot_status.get("coinPair")
            if positions and coin_pair:
                current_position = next((p for p in positions if p.get('coinPair') == coin_pair), None)
                if current_position:
                    bot_status["currentPosition"] = current_position
            
            # Add performance metrics if available
            if hasattr(app.ctx, 'PERFORMANCE_METRICS') and coin_pair and coin_pair in app.ctx.PERFORMANCE_METRICS:
                bot_status["performance"] = app.ctx.PERFORMANCE_METRICS[coin_pair]
            
            return json(
                standard_response(True, {
                    "bot": bot_status
                })
            )
        
        # Get all bot statuses (combined from both sources)
        bot_statuses = []
        processed_ids = set()
        
        # First process app.ctx.BOT_STATES
        for id, state in app.ctx.BOT_STATES.items():
            # Create copy to avoid modifying original
            status = state.copy()
            
            # Update with current position info if available
            coin_pair = status.get("coinPair")
            if positions and coin_pair:
                current_position = next((p for p in positions if p.get('coinPair') == coin_pair), None)
                if current_position:
                    status["currentPosition"] = current_position
            
            # Add performance metrics if available
            if hasattr(app.ctx, 'PERFORMANCE_METRICS') and coin_pair and coin_pair in app.ctx.PERFORMANCE_METRICS:
                status["performance"] = {
                    "totalTrades": app.ctx.PERFORMANCE_METRICS[coin_pair].get("totalTrades", 0),
                    "winRate": app.ctx.PERFORMANCE_METRICS[coin_pair].get("winRate", 0),
                    "netProfit": app.ctx.PERFORMANCE_METRICS[coin_pair].get("netProfit", 0),
                }
            
            bot_statuses.append(status)
            processed_ids.add(id)
        
        # Now add any bots from global BOT_STATES that weren't in app.ctx
        for id, state in BOT_STATES.items():
            if id not in processed_ids:
                # Create copy to avoid modifying original
                status = state.copy()
                
                # Update with current position info if available
                coin_pair = status.get("coinPair")
                if positions and coin_pair:
                    current_position = next((p for p in positions if p.get('coinPair') == coin_pair), None)
                    if current_position:
                        status["currentPosition"] = current_position
                
                # Add to list and app context for future
                bot_statuses.append(status)
                app.ctx.BOT_STATES[id] = status.copy()
                processed_ids.add(id)
                logger.info(f"[{request_id}] Added bot {id} from global state that was missing from app context")
        
        # Sort by start time (newest first)
        bot_statuses.sort(key=lambda x: x.get("startTime", 0), reverse=True)
        
        # Count active bots
        active_bots_count = sum(1 for b in bot_statuses if b.get("active", False))
        logger.info(f"[{request_id}] Active bots count: {active_bots_count}, total bots: {len(bot_statuses)}")
        
        # Debug dump of first active bot if any
        active_bots = [b for b in bot_statuses if b.get("active", False)]
        if active_bots:
            logger.info(f"[{request_id}] First active bot details: {json_lib.dumps(active_bots[0])}")
        
        return json(
            standard_response(True, {
                "bots": bot_statuses,
                "count": len(bot_statuses),
                "systemStatus": {
                    "activeBots": active_bots_count,
                    "maxBots": config.MAX_OPEN_POSITIONS,
                    "timestamp": int(time.time() * 1000),
                    "rateUsage": {
                        "minute": (app.ctx.RATE_LIMIT_STATE.get('minute_window', {}).get('weight', 0) / 1200 * 100) if hasattr(app.ctx, 'RATE_LIMIT_STATE') and 'minute_window' in app.ctx.RATE_LIMIT_STATE else 0,
                        "second": (app.ctx.RATE_LIMIT_STATE.get('second_window', {}).get('calls', 0) / 20 * 100) if hasattr(app.ctx, 'RATE_LIMIT_STATE') and 'second_window' in app.ctx.RATE_LIMIT_STATE else 0
                    }
                }
            })
        )
            
    except Exception as e:
        logger.error(f"[{request_id}] Error getting bot status: {str(e)}")
        logger.debug(f"[{request_id}] Exception traceback: {traceback.format_exc()}")
        return json(
            standard_response(False, error=f"Server error: {str(e)}"), 
            status=500
        )

@bot_bp.route("/performance", methods=["GET"])
@openapi.tag("Bot Control")
@openapi.summary("Get trading bot performance metrics")
@openapi.description("Get detailed performance metrics for a trading bot.")
@openapi.parameter("X-MBX-APIKEY", str, "header", required=True)
@openapi.parameter("X-MBX-APISECRET", str, "header", required=True)
@openapi.parameter("coinPair", str, "query", required=False, description="Trading pair to get metrics for, or 'all' for all pairs")
@openapi.parameter("botId", str, "query", required=False, description="Bot ID to get metrics for")
@openapi.response(200, {"application/json": {"success": bool, "metrics": dict}})
@openapi.response(400, {"application/json": {"success": bool, "error": str}})
@openapi.response(401, {"application/json": {"success": bool, "error": str}})
@openapi.response(404, {"application/json": {"success": bool, "error": str}})
@validate_api_key
async def get_performance_metrics(request: Request):
    """Get detailed performance metrics for a specific bot or trading pair."""
    app = request.app  # Access the Sanic app instance
    request_id = builtins.id(request)
    logger.info(f"[{request_id}] Processing performance metrics request")
    
    try:
        coin_pair = request.args.get("coinPair")
        bot_id = request.args.get("botId")
        
        if not coin_pair and not bot_id:
            logger.warning(f"[{request_id}] Either coinPair or botId is required")
            return json(
                standard_response(False, error="Either coinPair or botId is required"), 
                status=400
            )
        
        # Ensure app.ctx.PERFORMANCE_METRICS exists
        if not hasattr(app.ctx, 'PERFORMANCE_METRICS'):
            app.ctx.PERFORMANCE_METRICS = {}
            
        # Ensure app.ctx.BOT_STATES exists
        if not hasattr(app.ctx, 'BOT_STATES'):
            app.ctx.BOT_STATES = {}
            
        # Special case: handle 'all' coin pairs request
        if coin_pair and coin_pair.lower() == 'all':
            # Return aggregated metrics for all pairs
            if not app.ctx.PERFORMANCE_METRICS:
                # Return empty metrics if none are available
                return json(
                    standard_response(True, {
                        "metrics": {},
                        "allPairs": True,
                        "availablePairs": [],
                        "count": 0
                    })
                )
            
            # Return metrics for all available pairs
            all_metrics = {}
            for pair, metrics in app.ctx.PERFORMANCE_METRICS.items():
                all_metrics[pair] = metrics
            
            return json(
                standard_response(True, {
                    "metrics": all_metrics,
                    "allPairs": True,
                    "availablePairs": list(app.ctx.PERFORMANCE_METRICS.keys()),
                    "count": len(app.ctx.PERFORMANCE_METRICS)
                })
            )
        
        # If bot ID provided, get coin pair from bot state
        if bot_id:
            if bot_id not in app.ctx.BOT_STATES:
                logger.warning(f"[{request_id}] Bot {bot_id} not found")
                return json(
                    standard_response(False, error=f"Bot {bot_id} not found"), 
                    status=404
                )
            
            coin_pair = app.ctx.BOT_STATES[bot_id].get("coinPair")
            if not coin_pair:
                logger.warning(f"[{request_id}] Bot {bot_id} has no coin pair")
                return json(
                    standard_response(False, error=f"Bot {bot_id} has no coin pair"), 
                    status=400
                )
        
        # Get metrics for coin pair
        if coin_pair not in app.ctx.PERFORMANCE_METRICS:
            logger.warning(f"[{request_id}] No metrics found for {coin_pair}")
            return json(
                standard_response(True, {
                    "metrics": {},
                    "coinPair": coin_pair,
                    "message": f"No metrics found yet for {coin_pair}"
                })
            )
        
        metrics = app.ctx.PERFORMANCE_METRICS[coin_pair]
        
        # Get bot state if available
        bot_state = None
        if bot_id:
            bot_state = app.ctx.BOT_STATES.get(bot_id)
        else:
            # Find bot for this coin pair
            for id, state in app.ctx.BOT_STATES.items():
                if state.get("coinPair") == coin_pair:
                    bot_state = state
                    break
        
        return json(
            standard_response(True, {
                "metrics": metrics,
                "bot": bot_state,
                "coinPair": coin_pair
            })
        )
            
    except Exception as e:
        logger.error(f"[{request_id}] Error getting performance metrics: {str(e)}")
        logger.debug(f"[{request_id}] Exception traceback: {traceback.format_exc()}")
        return json(
            standard_response(False, error=f"Server error: {str(e)}"), 
            status=500
        )

@bot_bp.route("/events", methods=["GET"])
@openapi.tag("Bot Control")
@openapi.summary("Get trading bot events")
@openapi.description("Get event history for all bots or a specific bot.")
@openapi.parameter("X-MBX-APIKEY", str, "header", required=True)
@openapi.parameter("X-MBX-APISECRET", str, "header", required=True)
@openapi.parameter("botId", str, "query", required=False, description="Bot ID to filter events")
@openapi.parameter("limit", int, "query", required=False, description="Maximum number of events to return")
@openapi.parameter("eventType", str, "query", required=False, description="Type of events to filter")
@openapi.response(200, {"application/json": {"success": bool, "events": list, "count": int}})
@openapi.response(400, {"application/json": {"success": bool, "error": str}})
@openapi.response(401, {"application/json": {"success": bool, "error": str}})
@validate_api_key
async def get_bot_events(request: Request):
    """Get event history for all bots or a specific bot."""
    app = request.app  # Access the Sanic app instance
    request_id = builtins.id(request)
    logger.info(f"[{request_id}] Processing bot events request")
    
    try:
        # Parse query parameters
        bot_id = request.args.get("botId")
        event_type = request.args.get("eventType")
        
        limit_str = request.args.get("limit", "50")
        try:
            limit = int(limit_str)
            if limit <= 0:
                limit = 50
            elif limit > 1000:
                limit = 1000
        except ValueError:
            limit = 50
        
        # Ensure app.ctx.BOT_EVENT_HISTORY exists
        if not hasattr(app.ctx, 'BOT_EVENT_HISTORY'):
            app.ctx.BOT_EVENT_HISTORY = []
            
        # Filter events
        filtered_events = app.ctx.BOT_EVENT_HISTORY
        
        if bot_id:
            filtered_events = [
                e for e in filtered_events 
                if e.get("details", {}).get("botId") == bot_id
            ]
        
        if event_type:
            filtered_events = [
                e for e in filtered_events 
                if e.get("type") == event_type
            ]
        
        # Sort by timestamp (newest first) and limit
        filtered_events.sort(key=lambda x: x.get("timestamp", 0), reverse=True)
        filtered_events = filtered_events[:limit]
        
        return json(
            standard_response(True, {
                "events": filtered_events,
                "count": len(filtered_events),
                "totalEvents": len(app.ctx.BOT_EVENT_HISTORY)
            })
        )
            
    except Exception as e:
        logger.error(f"[{request_id}] Error getting bot events: {str(e)}")
        logger.debug(f"[{request_id}] Exception traceback: {traceback.format_exc()}")
        return json(
            standard_response(False, error=f"Server error: {str(e)}"), 
            status=500
        )

@bot_bp.route("/parameters", methods=["POST"])
@openapi.tag("Bot Control")
@openapi.summary("Update bot parameters")
@openapi.description("Update parameters for a running trading bot.")
@openapi.parameter("X-MBX-APIKEY", str, "header", required=True)
@openapi.parameter("X-MBX-APISECRET", str, "header", required=True)
@openapi.body({
    "application/json": {
        "botId": str, 
        "riskPercent": float, 
        "useTrailingStop": bool,
        "enableAutomation": bool
    }
}, required=True)
@openapi.response(200, {"application/json": {"success": bool, "message": str, "bot": dict}})
@openapi.response(400, {"application/json": {"success": bool, "error": str}})
@openapi.response(401, {"application/json": {"success": bool, "error": str}})
@openapi.response(404, {"application/json": {"success": bool, "error": str}})
@validate_api_key
async def update_bot_parameters(request: Request):
    """Update parameters for a running trading bot."""
    request_id = builtins.id(request)
    logger.info(f"[{request_id}] Processing update bot parameters request")
    
    try:
        data = request.json or {}
        bot_id = data.get("botId")
        
        if not bot_id:
            # If no bot ID provided, try to get the active bot
            if hasattr(request.app.ctx, "active_bot_id"):
                bot_id = request.app.ctx.active_bot_id
            else:
                logger.warning(f"[{request_id}] No botId provided and no active bot found")
                return json(
                    standard_response(False, error="No botId provided and no active bot running"), 
                    status=400
                )
        
        # Check if bot exists
        if bot_id not in BOT_STATES:
            logger.warning(f"[{request_id}] Bot {bot_id} not found")
            return json(
                standard_response(False, error=f"Bot {bot_id} not found"), 
                status=404
            )
        
        # Check if bot is running
        if not BOT_STATES[bot_id].get("active", False):
            logger.warning(f"[{request_id}] Bot {bot_id} is not running")
            return json(
                standard_response(False, error=f"Bot {bot_id} is not running"), 
                status=400
            )
        
        # Track changes
        changes = {}
        
        # Update risk percent if provided
        if "riskPercent" in data:
            risk_percent = float(data["riskPercent"])
            
            # Validate risk percent
            if risk_percent <= 0 or risk_percent > 5:
                logger.warning(f"[{request_id}] Invalid risk percent: {risk_percent}")
                return json(
                    standard_response(False, error="Risk percent must be between 0.1 and 5"), 
                    status=400
                )
            
            BOT_STATES[bot_id]["riskPercent"] = risk_percent
            changes["riskPercent"] = risk_percent
        
        # Update trailing stop setting if provided
        if "useTrailingStop" in data:
            use_trailing_stop = bool(data["useTrailingStop"])
            BOT_STATES[bot_id]["useTrailingStop"] = use_trailing_stop
            changes["useTrailingStop"] = use_trailing_stop
        
        # Update automation setting if provided
        if "enableAutomation" in data:
            enable_automation = bool(data["enableAutomation"])
            BOT_STATES[bot_id]["enableAutomation"] = enable_automation
            changes["enableAutomation"] = enable_automation
        
        # Update timestamp
        BOT_STATES[bot_id]["lastUpdate"] = int(time.time() * 1000)
        
        # Log event
        store_bot_event(
            "bot_parameters_updated", 
            {
                "botId": bot_id,
                "changes": changes,
                "coinPair": BOT_STATES[bot_id].get("coinPair")
            }
        )
        
        logger.info(f"[{request_id}] Updated parameters for bot {bot_id}: {changes}")
        return json(
            standard_response(True, {
                "message": "Bot parameters updated",
                "botId": bot_id,
                "changes": changes,
                "bot": BOT_STATES[bot_id]
            })
        )
            
    except Exception as e:
        logger.error(f"[{request_id}] Error updating bot parameters: {str(e)}")
        logger.debug(f"[{request_id}] Exception traceback: {traceback.format_exc()}")
        return json(
            standard_response(False, error=f"Server error: {str(e)}"), 
            status=500
        )

@bot_bp.websocket("/monitor/ws")
@openapi.tag("Bot Control")
@openapi.summary("Bot monitoring WebSocket")
@openapi.description("Real-time monitoring of trading bot activity via WebSocket.")
@openapi.parameter("X-MBX-APIKEY", str, "header", required=True, description="Binance API Key")
@openapi.parameter("X-MBX-APISECRET", str, "header", required=True, description="Binance API Secret")
@openapi.parameter("botId", str, "query", required=False, description="Optional bot ID to filter events")
async def bot_monitor_websocket(request: Request, ws):
    """WebSocket endpoint for real-time bot monitoring."""
    request_id = builtins.id(request)
    logger.info(f"[{request_id}] WebSocket connection opened for bot monitoring")
    
    # Validate API credentials
    api_key = request.headers.get("X-MBX-APIKEY")
    api_secret = request.headers.get("X-MBX-APISECRET")
    if not api_key or not api_secret:
        logger.warning(f"[{request_id}] WebSocket missing API credentials")
        await ws.send(json_lib.dumps(
            standard_response(False, error="Missing API credentials")
        ))
        await ws.close(1008, "Missing credentials")
        return
    
    # Parse query parameters
    bot_id = request.args.get("botId")
    
    # Send initial connection confirmation
    try:
        # Add to connections set
        BOT_WEBSOCKET_CONNECTIONS.add(ws)
        
        await ws.send(json_lib.dumps(
            standard_response(True, {
                "message": "Connected to bot monitoring stream",
                "configuration": {
                    "botId": bot_id if bot_id else "all"
                },
                "timestamp": int(time.time() * 1000)
            })
        ))
        
        # Send initial state
        if bot_id:
            # Send specific bot state
            if bot_id in BOT_STATES:
                await ws.send(json_lib.dumps({
                    "type": "bot_state",
                    "botId": bot_id,
                    "state": BOT_STATES[bot_id],
                    "timestamp": int(time.time() * 1000)
                }))
            else:
                await ws.send(json_lib.dumps({
                    "type": "error",
                    "error": f"Bot {bot_id} not found",
                    "timestamp": int(time.time() * 1000)
                }))
        else:
            # Send all bot states
            for id, state in BOT_STATES.items():
                await ws.send(json_lib.dumps({
                    "type": "bot_state",
                    "botId": id,
                    "state": state,
                    "timestamp": int(time.time() * 1000)
                }))
        
        # Send recent events
        filtered_events = BOT_EVENT_HISTORY
        if bot_id:
            filtered_events = [
                e for e in filtered_events 
                if e.get("details", {}).get("botId") == bot_id
            ]
        
        # Limit to recent events (last 20)
        recent_events = filtered_events[-20:]
        
        await ws.send(json_lib.dumps({
            "type": "recent_events",
            "events": recent_events,
            "count": len(recent_events),
            "timestamp": int(time.time() * 1000)
        }))
        
        # Set up heartbeat
        heartbeat_task = asyncio.create_task(send_heartbeats(ws))
        
        # Keep connection open until closed by client
        while not ws.closed:
            try:
                # Wait for a message from the client (or timeout)
                message = await asyncio.wait_for(ws.recv(), timeout=60)
                
                # Process client message (could be used for filtering or commands)
                try:
                    client_msg = json_lib.loads(message)
                    
                    # Handle potential client commands
                    command = client_msg.get("command")
                    if command == "filter":
                        # Update filter
                        bot_id = client_msg.get("botId")
                        logger.debug(f"[{request_id}] WebSocket filter updated to {bot_id}")
                    
                    # Send acknowledgment
                    await ws.send(json_lib.dumps({
                        "type": "command_acknowledgment",
                        "command": command,
                        "status": "received",
                        "timestamp": int(time.time() * 1000)
                    }))
                    
                except json_lib.JSONDecodeError:
                    logger.warning(f"[{request_id}] Invalid message from client: {message}")
                
            except asyncio.TimeoutError:
                # This is expected, just a way to periodically check if ws is closed
                continue
                
            except asyncio.CancelledError:
                logger.info(f"[{request_id}] WebSocket task cancelled")
                break
                
            except Exception as e:
                logger.error(f"[{request_id}] Error in WebSocket: {str(e)}")
                break
        
    except Exception as e:
        logger.error(f"[{request_id}] Unhandled exception in WebSocket: {str(e)}")
        logger.debug(f"[{request_id}] Exception traceback: {traceback.format_exc()}")
    
    finally:
        # Remove from connections set
        if ws in BOT_WEBSOCKET_CONNECTIONS:
            BOT_WEBSOCKET_CONNECTIONS.remove(ws)
            
        # Cancel heartbeat task
        if 'heartbeat_task' in locals() and not heartbeat_task.done():
            heartbeat_task.cancel()
            try:
                await heartbeat_task
            except asyncio.CancelledError:
                pass
                
        logger.info(f"[{request_id}] WebSocket connection for bot monitoring closed")
        
        # Ensure WebSocket is closed
        if not ws.closed:
            try:
                await ws.close(1000, "Connection terminated")
            except Exception:
                pass

async def send_heartbeats(ws):
    """Send periodic heartbeats to WebSocket client."""
    while not ws.closed:
        try:
            await ws.send(json_lib.dumps({
                "type": "heartbeat",
                "timestamp": int(time.time() * 1000)
            }))
            await asyncio.sleep(30)  # Send heartbeat every 30 seconds
        except Exception:
            break

@retry(
    stop=stop_after_attempt(5),
    wait=wait_fixed(5),
    retry=retry_if_exception_type((ConnectionError, TimeoutError))
)
async def trading_loop(app: Sanic, bot_id: str, coin_pair: str, strategy, risk_percent: float, 
                      use_trailing_stop: bool, enable_automation: bool):
    """Enhanced main trading loop for executing strategy-based trades with risk management."""
    # Add enhanced initial logging
    logger.info(f"Starting trading loop for bot {bot_id}, coin pair {coin_pair}")
    
    # Ensure both the app.ctx.BOT_STATES and the module global BOT_STATES are synchronized
    global BOT_STATES
    
    # Initialize app.ctx.BOT_STATES if it doesn't exist
    if not hasattr(app.ctx, 'BOT_STATES'):
        app.ctx.BOT_STATES = {}
    
    # Initialize module global BOT_STATES if it doesn't exist
    if BOT_STATES is None:
        BOT_STATES = {}
    
    # Ensure the bot is in both storages with consistent state
    bot_state = {
        "id": bot_id,
        "coinPair": coin_pair,
        "strategyName": strategy.__class__.__name__.lower(),
        "startTime": int(time.time() * 1000),
        "riskPercent": risk_percent,
        "useTrailingStop": use_trailing_stop,
        "active": True,
        "status": "Starting",
        "lastUpdate": int(time.time() * 1000),
        "currentPosition": None,
        "lastSignal": None,
        "enableAutomation": enable_automation,
        "error": None
    }
    
    # Update bot state in both places with identical data
    app.ctx.BOT_STATES[bot_id] = bot_state.copy()
    BOT_STATES[bot_id] = bot_state.copy()
    
    # Log bot state for verification after initialization
    logger.info(f"Initial BOT_STATES from app.ctx: {json_lib.dumps(app.ctx.BOT_STATES[bot_id])}")
    logger.info(f"Initial BOT_STATES from module global: {json_lib.dumps(BOT_STATES[bot_id])}")
    
    client = app.ctx.binance_client
    
    # Update bot state to Running in BOTH places with identical data
    update_time = int(time.time() * 1000)
    app.ctx.BOT_STATES[bot_id]["status"] = "Running"
    app.ctx.BOT_STATES[bot_id]["lastUpdate"] = update_time
    
    # Create a fresh copy to ensure no shared references
    BOT_STATES[bot_id] = app.ctx.BOT_STATES[bot_id].copy()
    
    logger.info(f"Updated BOT_STATES in app.ctx after setting to Running: {json_lib.dumps(app.ctx.BOT_STATES[bot_id])}")
    logger.info(f"Updated BOT_STATES in global after setting to Running: {json_lib.dumps(BOT_STATES[bot_id])}")
    
    # Also update app context instance variables for direct access
    app.ctx.trading_active = True
    app.ctx.current_coin_pair = coin_pair
    app.ctx.active_bot_id = bot_id
    
    # Verify that bot is properly marked as active in all locations
    app_ctx_active = app.ctx.BOT_STATES.get(bot_id, {}).get("active", False)
    global_active = BOT_STATES.get(bot_id, {}).get("active", False)
    
    if not app_ctx_active or not global_active:
        logger.error(f"Bot {bot_id} not properly marked as active! app_ctx: {app_ctx_active}, global: {global_active}")
        # Force correct active state in both places
        if bot_id in app.ctx.BOT_STATES:
            app.ctx.BOT_STATES[bot_id]["active"] = True
        if bot_id in BOT_STATES:
            BOT_STATES[bot_id]["active"] = True
    
    # Get or create risk manager
    if not hasattr(app.ctx, "risk_manager"):
        app.ctx.risk_manager = RiskManager(
            max_risk_percent=risk_percent,
            risk_reward_ratio=config.RISK_REWARD_RATIO,
            max_positions=config.MAX_OPEN_POSITIONS
        )
    
    risk_manager = app.ctx.risk_manager
    
    # Variables for trailing stop management
    entry_price = None
    stop_loss = None
    take_profit = None
    position_quantity = None
    last_check_time = 0
    last_signal_time = 0
    signal_cooldown = 300  # 5 minutes cooldown between signals
    last_indicator_update = 0
    
    # Performance tracking
    trade_count = 0
    successful_trades = 0
    failed_trades = 0
    
    # Track processed klines to avoid redundant calculations
    last_processed_kline_time = 0
    
    # Function to sync bot state between app context and module global
    def sync_bot_state():
        # Check both to find most accurate state
        app_ctx_bot = app.ctx.BOT_STATES.get(bot_id, {})
        global_bot = BOT_STATES.get(bot_id, {})
        
        # Determine active state (True if either is True)
        is_active = app_ctx_bot.get("active", False) or global_bot.get("active", False)
        
        # Use most recent status if available
        status = app_ctx_bot.get("status", "Unknown")
        if global_bot.get("lastUpdate", 0) > app_ctx_bot.get("lastUpdate", 0):
            status = global_bot.get("status", status)
        
        # Ensure consistent state in both places
        for storage in [app.ctx.BOT_STATES, BOT_STATES]:
            if bot_id in storage:
                storage[bot_id]["active"] = is_active
                storage[bot_id]["status"] = status
                storage[bot_id]["lastUpdate"] = int(time.time() * 1000)
        
        return is_active
    
    try:
        # Add iteration counter
        iteration_count = 0
        
        # Extra check that both active flags start as True
        sync_bot_state()
        
        # Primary trading loop
        while True:
            # Check both app context and global module for active state
            app_ctx_active = app.ctx.BOT_STATES.get(bot_id, {}).get("active", False)
            global_active = BOT_STATES.get(bot_id, {}).get("active", False)
            is_active = app_ctx_active or global_active
            
            # If bot is not active in either place, exit the loop
            if not is_active:
                logger.info(f"[Bot {bot_id}] Bot no longer active (app ctx: {app_ctx_active}, global: {global_active}), exiting loop")
                break
                
            try:
                # Add logging for initial iterations
                if iteration_count < 5:
                    logger.info(f"Trading loop iteration {iteration_count} for bot {bot_id}")
                    logger.info(f"Current BOT_STATES at start of iteration: {json_lib.dumps(app.ctx.BOT_STATES[bot_id])}")
                
                current_time = time.time()
                
                # Check for open positions (use caching to reduce API calls)
                logger.info(f"[Bot {bot_id}] Fetching open positions")
                open_positions = await get_cached_data(
                    client, 
                    'positions', 
                    fetch_func=client.get_open_positions
                )
                
                if open_positions is None:
                    logger.warning(f"[Bot {bot_id}] Failed to fetch positions, waiting before retry")
                    await asyncio.sleep(10)
                    continue
                
                logger.info(f"[Bot {bot_id}] Retrieved {len(open_positions) if open_positions else 0} open positions")
                current_position = next((p for p in open_positions if p.get('coinPair') == coin_pair), None)
                
                # Update bot state with position info in BOTH places at the same time
                update_time = int(current_time * 1000)
                for storage in [app.ctx.BOT_STATES, BOT_STATES]:
                    if bot_id in storage:
                        storage[bot_id]["currentPosition"] = current_position
                        storage[bot_id]["lastUpdate"] = update_time
                        # Always ensure active is true while loop is running
                        storage[bot_id]["active"] = True
                
                if iteration_count < 5:
                    logger.info(f"[Bot {bot_id}] Updated with position, now: {json_lib.dumps(app.ctx.BOT_STATES[bot_id])}")
                
                # Only check for new trades if automation is enabled
                if enable_automation:
                    if iteration_count < 5:
                        logger.info(f"[Bot {bot_id}] Automation is enabled, checking for signals")
                    
                    # Check if max positions reached
                    position_count = len(open_positions)
                    if position_count >= config.MAX_OPEN_POSITIONS and not current_position:
                        logger.debug(f"[Bot {bot_id}] Max open positions ({config.MAX_OPEN_POSITIONS}) reached, skipping signal check")
                        # Update bot state
                        new_status = "Waiting (Max Positions)"
                        for storage in [app.ctx.BOT_STATES, BOT_STATES]:
                            if bot_id in storage:
                                storage[bot_id]["status"] = new_status
                        await asyncio.sleep(30)  # Longer sleep when max positions reached
                        continue
                    
                    # Fetch market data (with caching to reduce API calls)
                    if iteration_count < 5:
                        logger.info(f"[Bot {bot_id}] Fetching market data for {coin_pair}")
                    
                    klines = await get_cached_data(
                        client, 
                        'klines', 
                        key=coin_pair,
                        fetch_func=client.get_klines,
                        symbol=coin_pair,
                        interval=config.KLINES_INTERVAL
                    )
                    
                    if not klines:
                        logger.warning(f"[Bot {bot_id}] Failed to fetch klines for {coin_pair}")
                        await asyncio.sleep(10)
                        continue
                    
                    if iteration_count < 5:
                        logger.info(f"[Bot {bot_id}] Retrieved {len(klines)} klines for {coin_pair}")
                    
                    # Get the timestamp of the most recent kline to check if there's new data
                    latest_kline_time = klines[-1][0]  # First element is timestamp
                    
                    # Only recalculate signals if we have new data (avoid redundant calculations)
                    if latest_kline_time > last_processed_kline_time:
                        if iteration_count < 5:
                            logger.info(f"[Bot {bot_id}] Processing new kline data")
                        
                        # Prepare DataFrame for strategy
                        df = pd.DataFrame(klines, columns=["timestamp", "open", "high", "low", "close", "volume"] + [""] * 6)
                        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")
                        for col in ["open", "high", "low", "close", "volume"]:
                            df[col] = pd.to_numeric(df[col])
                        
                        # Check if we have enough data
                        if len(df) < 30:  # Minimum data points for reliable signals
                            logger.warning(f"[Bot {bot_id}] Insufficient data for {coin_pair}: {len(df)} data points")
                            await asyncio.sleep(30)
                            continue
                        
                        # Update last processed kline time
                        last_processed_kline_time = latest_kline_time
                        
                        # Calculate signals
                        try:
                            if iteration_count < 5:
                                logger.info(f"[Bot {bot_id}] Calculating signals using strategy {strategy.__class__.__name__}")
                            
                            buy_signal, sell_signal = strategy.calculate_signals(df)
                            
                            if iteration_count < 5:
                                logger.info(f"[Bot {bot_id}] Signal results: buy={buy_signal}, sell={sell_signal}")
                            
                            # Store last signal in BOTH places
                            if buy_signal or sell_signal:
                                signal_data = {
                                    "type": "BUY" if buy_signal else "SELL",
                                    "timestamp": int(current_time * 1000),
                                    "price": float(df["close"].iloc[-1])
                                }
                                
                                for storage in [app.ctx.BOT_STATES, BOT_STATES]:
                                    if bot_id in storage:
                                        storage[bot_id]["lastSignal"] = signal_data
                                
                                logger.info(f"[Bot {bot_id}] Updated lastSignal: {json_lib.dumps(signal_data)}")
                            
                            # Process buy signal only if not in position and cooldown expired
                            if buy_signal and not current_position and (current_time - last_signal_time > signal_cooldown):
                                # Get account balance (cached)
                                balance = await get_cached_data(
                                    client,
                                    'account',
                                    fetch_func=client.get_account_balance
                                )
                                
                                if balance is None or balance <= 0:
                                    logger.warning(f"[Bot {bot_id}] Insufficient balance for buy")
                                else:
                                    # Get current price
                                    current_price = float(df["close"].iloc[-1])
                                    
                                    # Calculate stop loss (2% below entry)
                                    sl_percent = 2.0
                                    stop_loss_price = current_price * (1 - sl_percent / 100)
                                    
                                    # Use risk manager to calculate position size
                                    quantity = risk_manager.calculate_position_size(
                                        account_balance=balance,
                                        entry_price=current_price,
                                        stop_loss=stop_loss_price,
                                        symbol=coin_pair
                                    )
                                    
                                    # Calculate take profit based on risk-reward ratio
                                    take_profit_price = risk_manager.calculate_take_profit(
                                        entry_price=current_price,
                                        stop_loss=stop_loss_price
                                    )
                                    
                                    if quantity > 0:
                                        # Apply rate limiting before order
                                        await manage_rate_limits(client)
                                        
                                        # Create buy order
                                        order_id = await client.create_buy_order(
                                            coin_pair, 
                                            quantity, 
                                            current_price,
                                            stop_loss=stop_loss_price,
                                            take_profit=take_profit_price
                                        )
                                        
                                        if order_id:
                                            # Set position tracking variables
                                            entry_price = current_price
                                            stop_loss = stop_loss_price
                                            take_profit = take_profit_price
                                            position_quantity = quantity
                                            
                                            # Update strategy position state
                                            strategy.update_position(True, False)
                                            
                                            # Update last signal time
                                            last_signal_time = current_time
                                            
                                            # Invalidate positions cache to force refresh
                                            CACHE['positions']['timestamp'] = 0
                                            
                                            # Log event
                                            store_bot_event(
                                                "trade_opened", 
                                                {
                                                    "botId": bot_id,
                                                    "coinPair": coin_pair,
                                                    "side": "BUY",
                                                    "quantity": quantity,
                                                    "price": current_price,
                                                    "stopLoss": stop_loss_price,
                                                    "takeProfit": take_profit_price,
                                                    "orderId": order_id
                                                }
                                            )
                                            
                                            logger.info(f"Buy order placed: {order_id} for {quantity} {coin_pair} at {current_price}")
                                        else:
                                            logger.error(f"Buy order failed for {coin_pair}")
                                            failed_trades += 1
                                    else:
                                        logger.warning(f"Risk manager returned zero quantity for {coin_pair}")
                            
                            # Process sell signal only if in position and cooldown expired
                            elif sell_signal and current_position and (current_time - last_signal_time > signal_cooldown):
                                # Apply rate limiting before order
                                await manage_rate_limits(client)
                                
                                # Exit position
                                success = await client.exit_position(coin_pair)
                                if success:
                                    # Calculate profit
                                    entry = float(current_position.get('entryPrice', 0))
                                    exit_price = float(df["close"].iloc[-1])
                                    quantity = float(current_position.get('quantity', 0))
                                    profit = (exit_price - entry) * quantity if entry > 0 and quantity > 0 else 0
                                    
                                    # Update strategy position state
                                    strategy.update_position(False, True)
                                    
                                    # Update last signal time
                                    last_signal_time = current_time
                                    
                                    # Invalidate positions cache to force refresh
                                    CACHE['positions']['timestamp'] = 0
                                    
                                    # Update performance metrics
                                    update_performance_metrics(
                                        coin_pair,
                                        {
                                            "tradeCompleted": True,
                                            "profit": profit,
                                            "fees": 0,  # Estimate fees or get from API
                                            "entryPrice": entry,
                                            "exitPrice": exit_price,
                                            "quantity": quantity,
                                            "side": "SELL",
                                            "exitReason": "Signal"
                                        }
                                    )
                                    
                                    # Log event
                                    store_bot_event(
                                        "trade_closed", 
                                        {
                                            "botId": bot_id,
                                            "coinPair": coin_pair,
                                            "side": "SELL",
                                            "quantity": quantity,
                                            "entryPrice": entry,
                                            "exitPrice": exit_price,
                                            "profit": profit,
                                            "reason": "Signal"
                                        }
                                    )
                                    
                                    # Reset position tracking variables
                                    entry_price = None
                                    stop_loss = None
                                    take_profit = None
                                    position_quantity = None
                                    
                                    logger.info(f"Sell order executed for {coin_pair} with profit {profit}")
                                    successful_trades += 1
                                else:
                                    logger.error(f"Sell order failed for {coin_pair}")
                                    failed_trades += 1
                        
                        except Exception as e:
                            logger.error(f"[Bot {bot_id}] Error calculating signals: {str(e)}")
                            logger.error(f"[Bot {bot_id}] Exception details: {traceback.format_exc()}")
                    else:
                        logger.debug(f"Skipping signal calculation - no new kline data")
                
                # Update trailing stop if in position and enabled
                if current_position and use_trailing_stop:
                    current_price = float(current_position.get('currentPrice', 0))
                    entry_price_pos = float(current_position.get('entryPrice', 0))
                    sl_price = float(current_position.get('stopLoss', 0))
                    
                    if current_price > 0 and entry_price_pos > 0 and sl_price > 0:
                        # Calculate new trailing stop level
                        new_stop = risk_manager.calculate_trailing_stop(
                            entry_price=entry_price_pos,
                            current_price=current_price,
                            initial_stop=sl_price,
                            activation_percent=config.TRAILING_STOP_ACTIVATION,
                            trail_percent=0.5
                        )
                        
                        # Update stop loss if it changed significantly
                        if new_stop > sl_price * 1.005:  # 0.5% change
                            try:
                                # Apply rate limiting before order update
                                await manage_rate_limits(client)
                                
                                # Should implement this method in BinanceClient to update stop-loss orders
                                # await client.update_stop_loss(coin_pair, new_stop)
                                
                                # Log event
                                store_bot_event(
                                    "trailing_stop_updated", 
                                    {
                                        "botId": bot_id,
                                        "coinPair": coin_pair,
                                        "oldStop": sl_price,
                                        "newStop": new_stop,
                                        "currentPrice": current_price,
                                        "entryPrice": entry_price_pos
                                    }
                                )
                                
                                logger.info(f"Updated trailing stop for {coin_pair}: {sl_price} -> {new_stop}")
                            except Exception as e:
                                logger.error(f"Error updating trailing stop: {str(e)}")
                
                # Update bot status in BOTH places at the same time
                new_status = "In Position" if current_position else "Monitoring"
                update_time = int(time.time() * 1000)
                
                for storage in [app.ctx.BOT_STATES, BOT_STATES]:
                    if bot_id in storage:
                        storage[bot_id]["status"] = new_status
                        storage[bot_id]["lastUpdate"] = update_time
                        # CRUCIALLY - ensure both active flags stay true
                        storage[bot_id]["active"] = True
                
                # Dynamic sleep based on whether we're in a position and market volatility
                sleep_time = 10  # Base sleep time
                
                # If in position, check more frequently
                if current_position:
                    sleep_time = 5
                # If no position and low volatility (determined by signal presence), check less frequently
                elif not app.ctx.BOT_STATES[bot_id].get("lastSignal") or (current_time - app.ctx.BOT_STATES[bot_id].get("lastSignal", {}).get("timestamp", 0)/1000) > 1800:
                    sleep_time = 30
                
                # Log at the end of initial iterations
                if iteration_count < 5:
                    logger.info(f"[Bot {bot_id}] End of iteration {iteration_count}, sleeping for {sleep_time}s")
                    logger.info(f"[Bot {bot_id}] BOT_STATES from app.ctx after iteration: {json_lib.dumps(app.ctx.BOT_STATES[bot_id])}")
                    logger.info(f"[Bot {bot_id}] BOT_STATES from global after iteration: {json_lib.dumps(BOT_STATES[bot_id])}")
                
                iteration_count += 1
                await asyncio.sleep(sleep_time)
                
            except asyncio.CancelledError:
                logger.info(f"[Bot {bot_id}] Trading task for {coin_pair} cancelled")
                
                # Update bot state to reflect cancellation in BOTH places
                for storage in [app.ctx.BOT_STATES, BOT_STATES]:
                    if bot_id in storage:
                        storage[bot_id]["status"] = "Cancelled"
                        storage[bot_id]["active"] = False
                        storage[bot_id]["lastUpdate"] = int(time.time() * 1000)
                
                # Reset app.ctx variables
                app.ctx.trading_active = False
                app.ctx.current_coin_pair = None
                if getattr(app.ctx, "active_bot_id", None) == bot_id:
                    app.ctx.active_bot_id = None
                
                # Return early on cancellation
                return
                
            except Exception as e:
                logger.error(f"[Bot {bot_id}] Error in trading loop for {coin_pair}: {str(e)}")
                logger.debug(f"[Bot {bot_id}] Trading loop error traceback: {traceback.format_exc()}")
                
                # Update bot state with error in BOTH places
                error_time = int(time.time() * 1000)
                for storage in [app.ctx.BOT_STATES, BOT_STATES]:
                    if bot_id in storage:
                        storage[bot_id]["status"] = "Error"
                        storage[bot_id]["error"] = str(e)
                        storage[bot_id]["lastUpdate"] = error_time
                        # Crucially - maintain active flag as true during errors
                        storage[bot_id]["active"] = True
                
                # Log event
                store_bot_event(
                    "bot_error", 
                    {
                        "botId": bot_id,
                        "coinPair": coin_pair,
                        "error": str(e)
                    },
                    severity="error"
                )
                
                # Sleep before retry
                await asyncio.sleep(30)
        
        # Trading loop normally exited (not cancelled)
        logger.info(f"[Bot {bot_id}] Trading loop for {coin_pair} exited normally")
        
        # Update final bot state in BOTH places
        final_time = int(time.time() * 1000)
        for storage in [app.ctx.BOT_STATES, BOT_STATES]:
            if bot_id in storage:
                storage[bot_id]["status"] = "Stopped"
                storage[bot_id]["active"] = False
                storage[bot_id]["lastUpdate"] = final_time
        
        logger.info(f"[Bot {bot_id}] Final BOT_STATES: {json_lib.dumps(app.ctx.BOT_STATES[bot_id])}")
        
    except Exception as e:
        logger.error(f"[Bot {bot_id}] Fatal error in trading loop for {coin_pair}: {str(e)}")
        logger.debug(f"[Bot {bot_id}] Fatal error traceback: {traceback.format_exc()}")
        
        # Update bot state with fatal error in BOTH places
        error_time = int(time.time() * 1000)
        for storage in [app.ctx.BOT_STATES, BOT_STATES]:
            if bot_id in storage:
                storage[bot_id]["status"] = "Fatal Error"
                storage[bot_id]["active"] = False  # Set to inactive on fatal error
                storage[bot_id]["error"] = str(e)
                storage[bot_id]["lastUpdate"] = error_time
        
        # Reset app context variables
        app.ctx.trading_active = False
        app.ctx.current_coin_pair = None
        if getattr(app.ctx, "active_bot_id", None) == bot_id:
            app.ctx.active_bot_id = None
        
        # Log event
        store_bot_event(
            "bot_fatal_error", 
            {
                "botId": bot_id,
                "coinPair": coin_pair,
                "error": str(e)
            },
            severity="error"
        )