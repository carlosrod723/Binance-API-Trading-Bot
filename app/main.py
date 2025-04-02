# app/main.py

import os
import asyncio
import time
import httpx
import json
import logging
from dotenv import load_dotenv
from logging.handlers import RotatingFileHandler
from sanic import Sanic
from sanic.request import Request
from sanic.response import json as sanic_json, empty
from sanic.exceptions import SanicException
from sanic_ext import Extend
from sanic_limiter import Limiter, get_remote_address
from app.api.balance import balance_bp
from app.api.orders import orders_bp
from app.api.bot import bot_bp, BOT_STATES, PERFORMANCE_METRICS, BOT_EVENT_HISTORY
from app.api.positions import positions_bp
from app.api.strategy import strategy_bp, setup_strategy_registry
from app.api.market_data import marketdata_bp
from app.api.trading_pairs import trading_pairs_bp
from app.utils.binance_utils import BinanceClient
from binance.exceptions import BinanceAPIException
from app.utils.risk_manager import RiskManager
from app.config import config
import logging.config
import sys
import traceback
from datetime import datetime

# Add global API rate limit tracking
API_RATE_LIMIT_STATE = {
    'minute_window': {
        'calls': 0,
        'weight': 0,
        'last_reset': 0,
        'blocked_until': 0
    },
    'second_window': {
        'calls': 0,
        'last_reset': 0,
        'blocked_until': 0
    },
    'day_window': {
        'calls': 0,
        'weight': 0,
        'last_reset': 0
    }
}

# API call tracking function
async def track_api_call(weight=1, path=None):
    """Track API call for global rate limiting with improved frontend handling."""
    current_time = time.time()
    
    # Reset counters if window has passed
    if current_time - API_RATE_LIMIT_STATE['minute_window']['last_reset'] > 60:
        API_RATE_LIMIT_STATE['minute_window'] = {
            'calls': 0,
            'weight': 0,
            'last_reset': current_time,
            'blocked_until': 0
        }
    
    if current_time - API_RATE_LIMIT_STATE['second_window']['last_reset'] > 1:
        API_RATE_LIMIT_STATE['second_window'] = {
            'calls': 0,
            'last_reset': current_time,
            'blocked_until': 0
        }
    
    if current_time - API_RATE_LIMIT_STATE['day_window']['last_reset'] > 86400:
        API_RATE_LIMIT_STATE['day_window'] = {
            'calls': 0,
            'weight': 0,
            'last_reset': current_time
        }
    
    # Check if we're in a blocked period
    if current_time < API_RATE_LIMIT_STATE['minute_window']['blocked_until']:
        sleep_time = API_RATE_LIMIT_STATE['minute_window']['blocked_until'] - current_time
        logger.warning(f"Rate limit cooling off - sleeping for {sleep_time:.1f}s")
        await asyncio.sleep(sleep_time)
        # Recursive call after cooldown
        return await track_api_call(weight, path)
    
    # Update counters
    API_RATE_LIMIT_STATE['minute_window']['calls'] += 1
    API_RATE_LIMIT_STATE['minute_window']['weight'] += weight
    API_RATE_LIMIT_STATE['second_window']['calls'] += 1
    API_RATE_LIMIT_STATE['day_window']['calls'] += 1
    API_RATE_LIMIT_STATE['day_window']['weight'] += weight
    
    # Apply throttling if needed
    minute_weight = API_RATE_LIMIT_STATE['minute_window']['weight']
    second_calls = API_RATE_LIMIT_STATE['second_window']['calls']
    
    # Hard limits with safety margin
    MINUTE_WEIGHT_LIMIT = 1000  # Actual limit is 1200
    SECOND_CALL_LIMIT = 15      # Actual limit is 20
    
    # Identify dashboard paths for special handling
    is_dashboard_path = path and any(p in path for p in [
        '/api/v1/balance', 
        '/api/v1/positions', 
        '/api/v1/order/recent', 
        '/api/v1/bot/performance'
    ])
    
    if minute_weight > MINUTE_WEIGHT_LIMIT:
        # Special handling for dashboard requests during rate limiting
        if is_dashboard_path:
            logger.warning(f"Rate limit exceeded for dashboard path {path} - returning cooling down message")
            raise ValueError("API is cooling down due to rate limits. Please try again later.")
            
        # Implement exponential backoff for severe rate limiting (non-dashboard)
        sleep_time = 10 + (minute_weight - MINUTE_WEIGHT_LIMIT) * 0.5
        logger.warning(f"Minute weight limit exceeded ({minute_weight}/{MINUTE_WEIGHT_LIMIT}) - backing off for {sleep_time:.1f}s")
        
        # Set blocked flag
        API_RATE_LIMIT_STATE['minute_window']['blocked_until'] = current_time + sleep_time
        await asyncio.sleep(sleep_time)
        
    elif second_calls > SECOND_CALL_LIMIT:
        # Special handling for dashboard during second-based limits
        if is_dashboard_path:
            if second_calls > SECOND_CALL_LIMIT + 5:  # More severe overrun
                logger.warning(f"Second rate limit exceeded for dashboard path {path}")
                raise ValueError("API is cooling down due to rate limits. Please try again later.")
                
        # Add small delay for second-based rate limits (non-dashboard or minor overrun)
        sleep_time = 0.1 + (second_calls - SECOND_CALL_LIMIT) * 0.1
        logger.debug(f"Second call limit approaching ({second_calls}/{SECOND_CALL_LIMIT}) - adding {sleep_time:.2f}s delay")
        await asyncio.sleep(sleep_time)
    
    # Progressive throttling as we approach limits
    elif minute_weight > MINUTE_WEIGHT_LIMIT * 0.8:
        # Add progressively longer delays as we approach the limit
        factor = (minute_weight - (MINUTE_WEIGHT_LIMIT * 0.8)) / (MINUTE_WEIGHT_LIMIT * 0.2)
        sleep_time = 0.1 + factor * 0.9  # Up to 1 second delay at 100% of limit
        
        # Shorter delays for dashboard paths
        if is_dashboard_path:
            sleep_time = max(sleep_time * 0.5, 0.05)
            
        logger.debug(f"Approaching minute weight limit ({minute_weight}/{MINUTE_WEIGHT_LIMIT}) - adding {sleep_time:.2f}s delay")
        await asyncio.sleep(sleep_time)
    
    # Always add a tiny delay for heavy dashboard polling
    elif is_dashboard_path and API_RATE_LIMIT_STATE['second_window']['calls'] > 5:
        await asyncio.sleep(0.02)

# Create logs directory if it doesn't exist
os.makedirs("logs", exist_ok=True)

# Configure logging
LOGGING_CONFIG = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'standard': {
            'format': '%(asctime)s [%(levelname)s] %(name)s: %(message)s'
        },
        'detailed': {
            'format': '%(asctime)s [%(levelname)s] %(name)s:%(lineno)d: %(message)s'
        },
    },
    'handlers': {
        'console': {
            'level': 'INFO',
            'formatter': 'standard',
            'class': 'logging.StreamHandler',
            'stream': sys.stdout,
        },
        'file': {
            'level': 'DEBUG',
            'formatter': 'detailed',
            'class': 'logging.handlers.RotatingFileHandler',
            'filename': f'logs/tradingbot_{datetime.now().strftime("%Y%m%d")}.log',
            'maxBytes': 10485760,  # 10 MB
            'backupCount': 10,
        },
        'error_file': {
            'level': 'ERROR',
            'formatter': 'detailed',
            'class': 'logging.handlers.RotatingFileHandler',
            'filename': f'logs/error_{datetime.now().strftime("%Y%m%d")}.log',
            'maxBytes': 10485760,  # 10 MB
            'backupCount': 10,
        },
    },
    'loggers': {
        '': {  # root logger
            'handlers': ['console', 'file', 'error_file'],
            'level': 'INFO',
            'propagate': True
        },
        'app': {
            'handlers': ['console', 'file', 'error_file'],
            'level': 'DEBUG',
            'propagate': False
        },
    }
}

# Set logging level to WARNING except for login
logging.config.dictConfig(LOGGING_CONFIG)
logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)  

# Set warning filter for asyncio
if hasattr(asyncio, 'WindowsSelectorEventLoopPolicy'):
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

app = Sanic("TradingBotAPI")
Extend(app)

# Initialize BOT_STATES in the application context
app.ctx.BOT_STATES = {}
app.ctx.RATE_LIMIT_STATE = API_RATE_LIMIT_STATE

# Rate limiting
limiter = Limiter(app, key_func=get_remote_address)
limiter.limit("60/minute")(balance_bp)
limiter.limit("120/minute")(orders_bp)
limiter.limit("30/minute")(bot_bp)
limiter.limit("60/minute")(positions_bp)
limiter.limit("30/minute")(strategy_bp)
limiter.limit("120/minute")(marketdata_bp)
limiter.limit("30/minute")(trading_pairs_bp)

# CORS middleware
@app.middleware('request')
async def handle_options(request):
    if request.method == "OPTIONS":
        headers = {
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "GET, POST, PUT, DELETE, OPTIONS",
            "Access-Control-Allow-Headers": "Content-Type, X-MBX-APIKEY, X-MBX-APISECRET, Authorization",
            "Access-Control-Allow-Credentials": "true",
        }
        return empty(headers=headers, status=204)

@app.middleware('response')
async def add_cors_headers(request, response):
    response.headers["Access-Control-Allow-Origin"] = "*"
    response.headers["Access-Control-Allow-Credentials"] = "true"

# Register blueprints
app.blueprint(balance_bp)
app.blueprint(bot_bp)
app.blueprint(orders_bp)
app.blueprint(positions_bp)
app.blueprint(strategy_bp)
app.blueprint(marketdata_bp)
app.blueprint(trading_pairs_bp)

app.ctx.config = config

# Security middleware
@app.middleware('request')
async def add_security_headers(request):
    if not hasattr(request, 'ctx'):
        request.ctx = type('obj', (object,), {})

@app.middleware('response')
async def security_headers(request, response):
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["X-XSS-Protection"] = "1; mode=block"
    response.headers["Strict-Transport-Security"] = "max-age=31536000; includeSubDomains"
    response.headers["Content-Security-Policy"] = "default-src 'self'; script-src 'self'; object-src 'none'; frame-ancestors 'none'"
    if hasattr(request.ctx, 'correlation_id'):
        response.headers["X-Correlation-ID"] = request.ctx.correlation_id

@app.middleware('request')
async def add_correlation_id(request):
    request.ctx.correlation_id = f"request-{hash(request)}"
    logger.info(f"[{request.ctx.correlation_id}] {request.method} {request.path} - Starting")

@app.middleware('response')
async def log_response_time(request, response):
    if hasattr(request.ctx, 'correlation_id') and hasattr(request.ctx, 'timer_start'):
        elapsed = time.time() - request.ctx.timer_start
        logger.info(f"[{request.ctx.correlation_id}] {request.method} {request.path} - Completed in {elapsed:.3f}s with {response.status}")

@app.middleware('request')
async def start_timer(request):
    request.ctx.timer_start = time.time()

# Client initialization check middleware - ADD THIS NEW MIDDLEWARE
@app.middleware('request')
async def check_client_initialization(request):
    # Skip initialization check for certain paths
    skip_paths = [
        '/api/v1/test/connection',
        '/api/v1/initialization/status',
        '/health',
        '/api/v1/environment',
        '/api/v1/strategy/list',  # This is a static endpoint that doesn't need the client
        '/api/v1/marketdata',      # Allow partial marketdata access during initialization
        '/api/v1/trading-pairs'    # Allow symbol list access during initialization
    ]
    
    # Also skip for root path and OPTIONS requests
    if request.path == '/' or request.method == 'OPTIONS' or any(request.path.startswith(path) for path in skip_paths):
        return
    
    # Dashboard paths - provide more helpful status info
    dashboard_paths = [
        '/api/v1/balance',
        '/api/v1/positions',
        '/api/v1/order/recent'
    ]
    
    is_dashboard_request = any(request.path.startswith(path) for path in dashboard_paths)
    
    # Check if client is initialized
    if (not hasattr(request.app.ctx, "binance_client") or 
        not request.app.ctx.binance_client or 
        not getattr(request.app.ctx, "binance_initialized", False)):
        
        # Only log for non-polling requests to avoid excessive logs
        if not request.path.endswith('/health') and not request.path.endswith('/initialization/status'):
            logger.warning(f"Binance client not initialized for request: {request.method} {request.path}")
        
        # Special handling for dashboard requests
        if is_dashboard_request:
            # Return empty data with initialization status for dashboard components
            if request.path.startswith('/api/v1/balance'):
                return sanic_json({
                    "success": True,
                    "balance": 0,
                    "coins": [],
                    "initializing": True,
                    "timestamp": int(time.time() * 1000)
                })
            elif request.path.startswith('/api/v1/positions'):
                return sanic_json({
                    "success": True,
                    "positions": [],
                    "count": 0,
                    "initializing": True,
                    "timestamp": int(time.time() * 1000)
                })
            elif request.path.startswith('/api/v1/order/recent'):
                return sanic_json({
                    "success": True,
                    "orders": [],
                    "count": 0,
                    "initializing": True,
                    "timestamp": int(time.time() * 1000)
                })
        
        # Standard initialization error for other endpoints
        return sanic_json({
            "success": False,
            "error": "Service is initializing. Please wait...",
            "code": "INITIALIZING",
            "timestamp": int(time.time() * 1000),
            "retryIn": 2000  # Suggest frontend retry in 2 seconds
        }, status=503)

@app.exception(Exception)
async def handle_exception(request, exception):
    error_id = f"error-{int(time.time() * 1000)}"
    logger.error(f"Unhandled exception: {error_id} - {str(exception)}")
    logger.error(f"Exception details: {traceback.format_exc()}")
    status_code = exception.status_code if isinstance(exception, SanicException) else 500
    message = str(exception) if isinstance(exception, SanicException) else "Internal server error"
    return sanic_json({
        "success": False,
        "error": message,
        "errorId": error_id,
        "timestamp": int(time.time() * 1000)
    }, status=status_code)

async def setup_clients(app: Sanic) -> None:
    """Initialize Binance client with robust rate limiting, error handling, and auto-recovery."""
    try:
        # Add rate limit state to app context
        app.ctx.api_rate_limit_state = API_RATE_LIMIT_STATE
        
        # Worker staggering to prevent all workers hitting API at once
        worker_id = os.getpid()
        delay = (worker_id % 4) * 1.0 + 0.5  # Minimum 0.5s delay, max 3.5s
        logger.info(f"Worker {worker_id} delaying Binance client setup by {delay}s")
        await asyncio.sleep(delay)
        
        # Initially, no client until login
        app.ctx.binance_client = None
        app.ctx.binance_initialized = False
        app.ctx.binance_initialization_attempts = 0
        app.ctx.binance_last_attempt = 0
        app.ctx.binance_initialization_error = None
        app.ctx.binance_reconnection_attempts = 0
        app.ctx.binance_auto_reconnect_enabled = True  # Enable automatic reconnection attempts
        
        # Add a cache dictionary for API responses with TTL tracking
        app.ctx.api_cache = {
            'klines': {},
            'account': {'data': None, 'timestamp': 0, 'ttl': 60},  # 60 second TTL
            'positions': {'data': None, 'timestamp': 0, 'ttl': 60},
            'trades': {'data': None, 'timestamp': 0, 'ttl': 30},
            'symbols': {'data': None, 'timestamp': 0, 'ttl': 3600},  # 1 hour TTL
            'health': {'data': None, 'timestamp': 0, 'ttl': 10}  # 10 second TTL
        }
        
        # Add enhanced connection status tracking
        app.ctx.connection_status = {
            'last_successful_connection': 0,
            'last_failed_connection': 0,
            'consecutive_failures': 0,
            'lifetime_connections': 0,
            'lifetime_failures': 0,
            'current_status': 'waiting_for_login'
        }
        
        # Setup reconnection background task
        app.ctx.binance_reconnection_task = None
        
        # Add auto-recovery settings
        app.ctx.recovery_settings = {
            'max_retries': 5,
            'retry_delay': 5,  # seconds
            'max_retry_delay': 300,  # 5 minutes
            'last_retry_time': 0
        }
        
        # Add rate limiting function to app context
        app.ctx.track_api_call = track_api_call
        
        logger.info("Binance client setup deferred until login with auto-recovery enabled")
    except Exception as e:
        logger.error("Error setting up Binance client: %s", str(e))
        logger.error(traceback.format_exc())
        app.ctx.binance_client = None
        app.ctx.binance_initialized = False
        app.ctx.connection_status = {'current_status': 'setup_failed'} if not hasattr(app.ctx, 'connection_status') else app.ctx.connection_status

async def setup_risk_manager(app: Sanic) -> None:
    app.ctx.risk_manager = RiskManager(
        max_risk_percent=app.ctx.config.MAX_RISK_PERCENT_PER_TRADE,
        risk_reward_ratio=app.ctx.config.RISK_REWARD_RATIO,
        max_positions=app.ctx.config.MAX_OPEN_POSITIONS
    )
    logger.info("Risk manager initialized with max risk: %.2f%%", app.ctx.config.MAX_RISK_PERCENT_PER_TRADE)

async def configure_logging(app: Sanic) -> None:
    log_level = getattr(logging, app.ctx.config.LOG_LEVEL.upper(), logging.INFO)
    logging.getLogger().setLevel(log_level)
    logger.info("Logging configured with level: %s", app.ctx.config.LOG_LEVEL)

async def validate_config(app: Sanic) -> None:
    if not app.ctx.config.validate_config(strict=True):
        raise SanicException("Configuration validation failed", status_code=500)

async def initialize_monitoring(app: Sanic) -> None:
    app.ctx.bot_states = BOT_STATES
    app.ctx.performance_metrics = PERFORMANCE_METRICS
    app.ctx.bot_events = BOT_EVENT_HISTORY
    app.ctx.background_tasks = set()
    app.ctx.bot_events.append({
        "timestamp": int(time.time() * 1000),
        "type": "system_startup",
        "severity": "info",
        "details": {
            "version": "1.0.0",
            "mode": app.ctx.config.ENV_MODE,
            "pid": os.getpid(),
            "maxPositions": app.ctx.config.MAX_OPEN_POSITIONS
        }
    })
    logger.info("Monitoring subsystems initialized")

async def state_cleanup_task(app: Sanic) -> None:
    """Periodically clean up stale bot states."""
    while True:
        try:
            current_time = time.time()
            stale_bots = [
                bot_id for bot_id, state in app.ctx.bot_states.items()
                if not state.get("active", False) and current_time - state.get("lastUpdate", 0) / 1000 > 86400
            ]
            for bot_id in stale_bots:
                app.ctx.bot_states.pop(bot_id, None)
                logger.info(f"Removed stale bot state: {bot_id}")
            if len(app.ctx.bot_events) > 1000:
                app.ctx.bot_events = app.ctx.bot_events[-1000:]
        except asyncio.CancelledError:
            logger.info("State cleanup task cancelled")
            break
        except Exception as e:
            logger.error(f"Error in state cleanup task: {str(e)}")
        await asyncio.sleep(3600)  # Run every hour

async def websocket_ping_task(app: Sanic) -> None:
    """Periodically send ping to all WebSocket connections."""
    while True:
        try:
            if hasattr(app.ctx, 'bot_bp') and hasattr(app.ctx.bot_bp, 'BOT_WEBSOCKET_CONNECTIONS'):
                connections = list(app.ctx.bot_bp.BOT_WEBSOCKET_CONNECTIONS)
                for ws in connections:
                    try:
                        if not ws.closed:
                            await ws.send(json.dumps({
                                "type": "ping",
                                "timestamp": int(time.time() * 1000)
                            }))
                    except Exception as e:
                        logger.debug(f"WebSocket ping failed: {str(e)}")
        except asyncio.CancelledError:
            logger.info("WebSocket ping task cancelled")
            break
        except Exception as e:
            logger.error(f"Error in WebSocket ping task: {str(e)}")
        await asyncio.sleep(30)  # Send ping every 30 seconds

async def binance_auto_reconnect_task(app: Sanic) -> None:
    """Background task to automatically reconnect to Binance if connection is lost."""
    while True:
        try:
            if not hasattr(app.ctx, 'binance_auto_reconnect_enabled') or not app.ctx.binance_auto_reconnect_enabled:
                await asyncio.sleep(30)
                continue
                
            # Check if we have a client but it's not properly initialized
            if (hasattr(app.ctx, 'binance_client') and 
                app.ctx.binance_client is not None and 
                not getattr(app.ctx, 'binance_initialized', False)):
                
                # Don't retry too frequently
                current_time = time.time()
                recovery_settings = getattr(app.ctx, 'recovery_settings', {
                    'retry_delay': 30, 
                    'last_retry_time': 0,
                    'max_retries': 5
                })
                
                time_since_last_retry = current_time - recovery_settings.get('last_retry_time', 0)
                retry_delay = recovery_settings.get('retry_delay', 30)
                
                if time_since_last_retry < retry_delay:
                    await asyncio.sleep(5)  # Check again soon
                    continue
                    
                # Track attempt
                app.ctx.binance_reconnection_attempts = getattr(app.ctx, 'binance_reconnection_attempts', 0) + 1
                recovery_settings['last_retry_time'] = current_time
                
                # Don't try forever
                if app.ctx.binance_reconnection_attempts > recovery_settings.get('max_retries', 5):
                    logger.warning(f"Auto-reconnect reached max attempts ({recovery_settings.get('max_retries', 5)}). Pausing reconnection attempts.")
                    # Exponential backoff for retries after max attempts
                    recovery_settings['retry_delay'] = min(
                        recovery_settings.get('retry_delay', 30) * 2,
                        recovery_settings.get('max_retry_delay', 300)
                    )
                    # Reset counter but increase delay
                    app.ctx.binance_reconnection_attempts = 0
                    await asyncio.sleep(60)
                    continue
                
                # Get credentials
                logger.info(f"Auto-reconnect attempt #{app.ctx.binance_reconnection_attempts}")
                try:
                    # Try to re-initialize the client
                    await app.ctx.binance_client.initialize()
                    
                    # Success!
                    app.ctx.binance_initialized = True
                    app.ctx.connection_status['consecutive_failures'] = 0
                    app.ctx.connection_status['last_successful_connection'] = current_time
                    app.ctx.connection_status['current_status'] = 'connected'
                    app.ctx.connection_status['lifetime_connections'] += 1
                    
                    # Reset recovery parameters
                    recovery_settings['retry_delay'] = recovery_settings.get('initial_retry_delay', 30)
                    app.ctx.binance_reconnection_attempts = 0
                    logger.info("Auto-reconnect successful!")
                    
                    # Update status in bot events
                    if hasattr(app.ctx, 'bot_events'):
                        app.ctx.bot_events.append({
                            "timestamp": int(current_time * 1000),
                            "type": "connection_restored",
                            "severity": "info",
                            "details": {
                                "auto_reconnect": True,
                                "previous_failures": app.ctx.connection_status.get('consecutive_failures', 0)
                            }
                        })
                    
                except Exception as e:
                    # Track failure
                    app.ctx.connection_status['consecutive_failures'] = app.ctx.connection_status.get('consecutive_failures', 0) + 1
                    app.ctx.connection_status['last_failed_connection'] = current_time
                    app.ctx.connection_status['current_status'] = 'connection_failed'
                    app.ctx.connection_status['lifetime_failures'] += 1
                    logger.error(f"Auto-reconnect attempt failed: {str(e)}")
                    
                    # Update status in bot events
                    if hasattr(app.ctx, 'bot_events'):
                        app.ctx.bot_events.append({
                            "timestamp": int(current_time * 1000),
                            "type": "connection_failure",
                            "severity": "error",
                            "details": {
                                "error": str(e),
                                "consecutive_failures": app.ctx.connection_status.get('consecutive_failures', 0),
                                "attempt": app.ctx.binance_reconnection_attempts
                            }
                        })
            
            # Check less frequently if everything is fine
            elif getattr(app.ctx, 'binance_initialized', False):
                await asyncio.sleep(60)  # Check once per minute when connected
            else:
                await asyncio.sleep(15)  # Check more frequently when not connected
                
        except asyncio.CancelledError:
            logger.info("Binance auto-reconnect task cancelled")
            break
        except Exception as e:
            logger.error(f"Error in Binance auto-reconnect task: {str(e)}")
            # Don't crash the task on errors
            await asyncio.sleep(30)

async def setup_background_tasks(app: Sanic) -> None:
    # State cleanup task
    app.ctx.state_cleanup_task = asyncio.create_task(state_cleanup_task(app))
    app.ctx.background_tasks.add(app.ctx.state_cleanup_task)
    app.ctx.state_cleanup_task.add_done_callback(app.ctx.background_tasks.discard)
    
    # WebSocket ping task
    app.ctx.ws_ping_task = asyncio.create_task(websocket_ping_task(app))
    app.ctx.background_tasks.add(app.ctx.ws_ping_task)
    app.ctx.ws_ping_task.add_done_callback(app.ctx.background_tasks.discard)
    
    # Cache cleanup task
    app.ctx.cache_cleanup_task = asyncio.create_task(cache_cleanup_task(app))
    app.ctx.background_tasks.add(app.ctx.cache_cleanup_task)
    app.ctx.cache_cleanup_task.add_done_callback(app.ctx.background_tasks.discard)
    
    # Binance auto-reconnect task
    app.ctx.binance_reconnect_task = asyncio.create_task(binance_auto_reconnect_task(app))
    app.ctx.background_tasks.add(app.ctx.binance_reconnect_task)
    app.ctx.binance_reconnect_task.add_done_callback(app.ctx.background_tasks.discard)
    
    logger.info("Background tasks started")

async def cache_cleanup_task(app: Sanic) -> None:
    """Periodically clean up old cache entries."""
    while True:
        try:
            if hasattr(app.ctx, 'api_cache'):
                current_time = time.time()
                # Clean up klines cache
                if 'klines' in app.ctx.api_cache:
                    # Remove entries older than 5 minutes
                    keys_to_remove = []
                    for key, entry in app.ctx.api_cache['klines'].items():
                        if current_time - entry.get('timestamp', 0) > 300:  # 5 minutes
                            keys_to_remove.append(key)
                    
                    for key in keys_to_remove:
                        app.ctx.api_cache['klines'].pop(key, None)
                    
                    # If still too many entries, remove oldest
                    if len(app.ctx.api_cache['klines']) > 200:
                        # Sort by timestamp
                        sorted_keys = sorted(
                            app.ctx.api_cache['klines'].keys(),
                            key=lambda k: app.ctx.api_cache['klines'][k].get('timestamp', 0)
                        )
                        # Remove oldest 20%
                        for key in sorted_keys[:40]:  # 20% of 200
                            app.ctx.api_cache['klines'].pop(key, None)
        except asyncio.CancelledError:
            logger.info("Cache cleanup task cancelled")
            break
        except Exception as e:
            logger.error(f"Error in cache cleanup task: {str(e)}")
        await asyncio.sleep(300)  # Run every 5 minutes

async def cleanup_background_tasks(app: Sanic) -> None:
    for task in app.ctx.background_tasks:
        if not task.done():
            task.cancel()
    if app.ctx.background_tasks:
        await asyncio.gather(*app.ctx.background_tasks, return_exceptions=True)
    logger.info("Background tasks cleaned up")

async def cleanup_clients(app: Sanic) -> None:
    if hasattr(app.ctx, "binance_client") and app.ctx.binance_client:
        await app.ctx.binance_client.close()
        logger.info("Binance client closed")

@app.route("/api/v1/initialization/status", methods=["GET", "OPTIONS"])
async def check_initialization(request: Request):
    """Add new endpoint to check client initialization status."""
    # Handle OPTIONS requests explicitly
    if request.method == "OPTIONS":
        headers = {
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "GET, OPTIONS",
            "Access-Control-Allow-Headers": "Content-Type, X-MBX-APIKEY, X-MBX-APISECRET, Authorization",
            "Access-Control-Allow-Credentials": "true",
        }
        return empty(headers=headers, status=204)
        
    is_initialized = (
        hasattr(request.app.ctx, "binance_client") and 
        request.app.ctx.binance_client and 
        getattr(request.app.ctx, "binance_initialized", False)
    )
    
    response = sanic_json({
        "initialized": is_initialized,
        "timestamp": int(time.time() * 1000)
    })
    
    # Add CORS headers for this endpoint specifically
    response.headers["Access-Control-Allow-Origin"] = "*"
    response.headers["Access-Control-Allow-Credentials"] = "true"
    
    return response

@app.route("/api/v1/test/ping", methods=["GET"])
async def test_ping(request: Request):
    logger.info("Received test ping request")
    return sanic_json({"success": True, "message": "Ping successful", "timestamp": int(time.time() * 1000)})

@app.route("/")
async def test(request: Request):
    return sanic_json({
        "name": "TradingBotAPI",
        "version": "1.0.0",
        "status": "running", 
        "mode": app.ctx.config.ENV_MODE,
        "timestamp": int(time.time() * 1000)
    })

@app.route("/health", methods=["GET", "OPTIONS"])
async def health_check(request: Request):
    # Handle OPTIONS requests explicitly
    if request.method == "OPTIONS":
        headers = {
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "GET, OPTIONS",
            "Access-Control-Allow-Headers": "Content-Type, X-MBX-APIKEY, X-MBX-APISECRET, Authorization",
            "Access-Control-Allow-Credentials": "true",
        }
        return empty(headers=headers, status=204)
        
    # Use cached health data if available and recent (last 10 seconds)
    current_time = time.time()
    if (hasattr(request.app.ctx, 'api_cache') and 
        'health' in request.app.ctx.api_cache and 
        current_time - request.app.ctx.api_cache['health'].get('timestamp', 0) < 10):
        
        response = sanic_json(request.app.ctx.api_cache['health']['data'])
        # Add CORS headers
        response.headers["Access-Control-Allow-Origin"] = "*"
        response.headers["Access-Control-Allow-Credentials"] = "true"
        return response
    
    client_healthy = False
    client_initialized = False
    client_health = None
    status_message = "Not connected"
    
    if hasattr(request.app.ctx, "binance_client") and request.app.ctx.binance_client:
        try:
            # Throttle API calls
            if hasattr(request.app.ctx, "track_api_call"):
                await request.app.ctx.track_api_call(weight=1)
                
            # Only do a ping, which is lightweight
            await request.app.ctx.binance_client.client.ping()
            client_healthy = True
            client_initialized = getattr(request.app.ctx, "binance_initialized", False)
            status_message = "Connected with account access"
            client_health = request.app.ctx.binance_client.get_health_metrics()
        except Exception as e:
            status_message = f"Connection error: {str(e)}"
    
    active_bots = sum(1 for state in request.app.ctx.bot_states.values() if state.get("active", False))
    memory_info = None
    try:
        import psutil
        process = psutil.Process(os.getpid())
        memory_info = {
            "rss": process.memory_info().rss / 1024 / 1024,
            "vms": process.memory_info().vms / 1024 / 1024,
            "percent": process.memory_percent()
        }
    except ImportError:
        memory_info = {"error": "psutil not available"}
    
    status = "healthy" if client_healthy and client_initialized else "degraded"
    uptime = current_time - request.app.ctx.started_at if hasattr(request.app.ctx, "started_at") else None
    
    # Include rate limit info in health check
    rate_limit_info = None
    if hasattr(request.app.ctx, 'api_rate_limit_state'):
        rate_limit_info = {
            'minute_weight': request.app.ctx.api_rate_limit_state['minute_window']['weight'],
            'minute_calls': request.app.ctx.api_rate_limit_state['minute_window']['calls'],
            'second_calls': request.app.ctx.api_rate_limit_state['second_window']['calls'],
            'day_calls': request.app.ctx.api_rate_limit_state['day_window']['calls']
        }
    
    health_data = {
        "success": True,
        "status": status,
        "timestamp": int(current_time * 1000),
        "uptime": uptime,
        "binance": {
            "connected": client_healthy,
            "initialized": client_initialized,
            "status": status_message,
            "clientHealth": client_health if client_healthy else None,
            "rateLimits": rate_limit_info
        },
        "bots": {
            "active": active_bots,
            "total": len(request.app.ctx.bot_states)
        },
        "system": {
            "memory": memory_info,
            "pid": os.getpid()
        }
    }
    
    # Cache the response
    if hasattr(request.app.ctx, 'api_cache'):
        request.app.ctx.api_cache['health'] = {
            'data': health_data,
            'timestamp': current_time
        }
    
    response = sanic_json(health_data)
    # Add CORS headers
    response.headers["Access-Control-Allow-Origin"] = "*"
    response.headers["Access-Control-Allow-Credentials"] = "true"
    return response

@app.middleware('request')
async def log_request_details(request):
    if request.path == '/api/v1/test/connection':
        logger.info(f"Request: {request.method} {request.path} - Headers: {dict(request.headers)} - Body: {request.json if request.body else None}")
    request.ctx.correlation_id = f"request-{hash(request)}"
    if request.path == '/api/v1/test/connection':
        logger.info(f"[{request.ctx.correlation_id}] {request.method} {request.path} - Starting")

@app.route("/api/v1/test/connection", methods=["POST"])
@limiter.limit("10/minute") # Keep rate limit
async def test_connection(request: Request):
    try:
        correlation_id = request.ctx.correlation_id if hasattr(request.ctx, 'correlation_id') else 'unknown'
        logger.info(f"[{correlation_id}] Received /test/connection request")
        data = request.json
        api_key = data.get("apiKey")
        api_secret = data.get("apiSecret")
        # --- Get the isTestnet flag from the frontend request ---
        # IMPORTANT: We check explicitly for False since JSON booleans become Python booleans
        is_testnet_from_frontend = data.get("isTestnet")
        # Log the exact value received for debugging
        logger.info(f"[{correlation_id}] Received isTestnet value: {is_testnet_from_frontend} (type: {type(is_testnet_from_frontend)})")
        # Only set to False if explicitly False
        is_testnet_from_frontend = False if is_testnet_from_frontend is False else True

        logger.info(f"[{correlation_id}] Credentials received - apiKey: {api_key[:5] if api_key and len(api_key) >= 5 else 'N/A'}..., apiSecret length: {len(api_secret) if api_secret else 0}, isTestnet requested: {is_testnet_from_frontend}")

        if not api_key or not api_secret:
            logger.warning(f"[{correlation_id}] Missing API key or secret")
            return sanic_json({"success": False, "error": "API key and secret are required"}, status=400)

        # --- Check Cooldown ---
        current_time = time.time()
        if (hasattr(request.app.ctx, 'binance_last_attempt') and
            current_time - request.app.ctx.binance_last_attempt < 5.0): # 5 second cooldown
            cooldown = 5.0 - (current_time - request.app.ctx.binance_last_attempt)
            logger.warning(f"[{correlation_id}] Connection attempt too soon. Wait {cooldown:.1f}s.")
            return sanic_json({
                "success": False,
                "error": f"Please wait {cooldown:.1f} seconds before attempting to connect again",
                "cooldown": True,
                "remainingTime": cooldown
            }, status=429)

        # --- Update attempt timestamp ---
        request.app.ctx.binance_initialization_attempts = getattr(request.app.ctx, 'binance_initialization_attempts', 0) + 1
        request.app.ctx.binance_last_attempt = current_time
        logger.debug(f"[{correlation_id}] Initialization attempt #{request.app.ctx.binance_initialization_attempts}")

        # --- Close Existing Client if present ---
        if hasattr(request.app.ctx, "binance_client") and request.app.ctx.binance_client:
            logger.info(f"[{correlation_id}] Closing existing Binance client before new connection attempt.")
            await request.app.ctx.binance_client.close()

        # --- Reset state flags in app context ---
        request.app.ctx.binance_client = None
        request.app.ctx.binance_initialized = False
        request.app.ctx.current_env_is_testnet = None # Reset active environment flag
        request.app.ctx.binance_initialization_error = None

        # --- Clear Cache (Optional but recommended on switching env/keys) ---
        if hasattr(request.app.ctx, 'api_cache'):
             logger.debug(f"[{correlation_id}] Clearing API cache on new connection attempt")
             # Add logic here to clear relevant cache entries if needed

        # --- Create and Validate Test Client based on Frontend Flag ---
        testnet_mode = is_testnet_from_frontend # Use the flag from the frontend!
        active_environment_name = "Testnet" if testnet_mode else "Live"
        logger.info(f"[{correlation_id}] Attempting connection validation in {active_environment_name} mode.")

        # Use a temporary client instance for validation
        validation_client = BinanceClient(api_key, api_secret, testnet=testnet_mode)

        try:
            # Initialize the validation client (includes internal basic checks)
            await validation_client.initialize()
            logger.info(f"[{correlation_id}] validation_client.initialize() completed for {active_environment_name}.")

            # Explicitly test core functions again with this specific client instance
            logger.info(f"[{correlation_id}] Pinging Binance ({active_environment_name})")
            await validation_client.client.ping()
            await asyncio.sleep(0.1) # Small delay

            logger.info(f"[{correlation_id}] Fetching server time ({active_environment_name})")
            time_result = await validation_client.client.get_server_time()
            await asyncio.sleep(0.1)

            logger.info(f"[{correlation_id}] Testing account access via get_account() ({active_environment_name})")
            account = await validation_client.client.get_account() # This is the critical check
            logger.info(f"[{correlation_id}] get_account() successful for {active_environment_name}.")
            account_status = "API key valid with account access"

            # --- Success! Update App Context with the validated client and environment ---
            request.app.ctx.binance_client = validation_client # Assign the validated client
            request.app.ctx.binance_initialized = True
            request.app.ctx.current_env_is_testnet = testnet_mode # Store the active environment
            request.app.ctx.binance_initialization_error = None # Clear any previous error
            logger.info(f"[{correlation_id}] Binance client fully initialized and set in app context. Active environment: {active_environment_name}")

            # --- Reset Rate Limits (Optional but good practice) ---
            if hasattr(request.app.ctx, 'api_rate_limit_state'):
                logger.debug(f"[{correlation_id}] Resetting API rate limit counters on successful connection")
                # Add logic to reset counters if desired

            # --- Prepare and Return Success Response ---
            response_data = {
                "success": True,
                "environment": "testnet" if testnet_mode else "live", # Confirm the active environment
                "testnet": testnet_mode, # Send boolean back for consistency
                "initialized": True,
                "serverTime": time_result,
                "accountStatus": account_status,
                "attempt": request.app.ctx.binance_initialization_attempts
            }
            logger.info(f"[{correlation_id}] Connection test successful for {active_environment_name}")
            return sanic_json(response_data)

        except Exception as e:
            # --- Handle Validation Failure ---
            error_message = f"API key validation failed for {active_environment_name}: {str(e)}"
            status_code = 401 # Default to Unauthorized

            # Check for specific Binance API errors for better feedback
            if isinstance(e, BinanceAPIException):
                error_message = f"Binance API Error [{e.code}] ({active_environment_name}): {e.message}"
                if e.code == -2015: status_code = 401 # Invalid API-key, IP, etc.
                elif e.code == -1022: status_code = 401 # Signature for this request is not valid (likely wrong secret)
                elif e.code == -2014: status_code = 400 # API-key format invalid.
                # Add more specific codes if needed (e.g., -2008 invalid symbol might occur during initialize)
            elif isinstance(e, (ConnectionError, TimeoutError, asyncio.TimeoutError)):
                 error_message = f"Network Error: Could not connect to Binance {active_environment_name} API. {str(e)}"
                 status_code = 503 # Service Unavailable

            logger.error(f"[{correlation_id}] {error_message}", exc_info=True) # Log full traceback for errors
            await validation_client.close() # Ensure temporary client is closed on failure
            request.app.ctx.binance_initialization_error = error_message # Store error message
            return sanic_json({
                "success": False,
                "error": error_message,
                "details": {
                    "attempt": request.app.ctx.binance_initialization_attempts,
                    "timestamp": int(current_time * 1000)
                }
            }, status=status_code)

    except Exception as e:
        # --- Handle unexpected errors in the handler structure ---
        correlation_id = request.ctx.correlation_id if hasattr(request.ctx, 'correlation_id') else 'outer_unknown'
        logger.error(f"[{correlation_id}] Unexpected error in /test/connection handler: {str(e)}", exc_info=True)
        # Ensure client context is reset even on outer errors
        request.app.ctx.binance_client = None
        request.app.ctx.binance_initialized = False
        request.app.ctx.current_env_is_testnet = None
        return sanic_json({"success": False, "error": "Internal server error during connection test"}, status=500)


@app.route("/api/v3/ping", methods=["GET"])
@limiter.limit("60/minute")
async def ping_binance(request):
    async with httpx.AsyncClient() as client:
        response = await client.get("https://api.binance.com/api/v3/ping")
        return sanic_json(response.json() if response.status_code == 200 else {"error": "Failed to ping Binance"}, 
                         status=response.status_code)

@app.route("/api/v1/environment", methods=["GET"])
async def get_environment(request: Request):
    return sanic_json({
        "mode": request.app.ctx.config.ENV_MODE,
        "is_production": request.app.ctx.config.is_production(),
        "server_time": int(time.time() * 1000)
    })

@app.before_server_start
async def before_start(app, loop):
    logger.info("Starting TradingBotAPI server...")
    app.ctx.started_at = time.time()

    if app.ctx.config.ENV_MODE == "testnet":
        try:
            from app.utils.csv_utils import CSVBalanceHistoryManager
            csv_manager = CSVBalanceHistoryManager()
            if not csv_manager.file_exists():
                logger.info("Creating CSV balance history file for testnet")
                csv_manager.write_entry(
                    timestamp=datetime.now(),
                    balance=100000.0,
                    currency="USDT"
                )
        except Exception as e:
            logger.warning(f"Failed to initialize CSV balance history: {str(e)}")

app.register_listener(configure_logging, "before_server_start")
app.register_listener(validate_config, "before_server_start")
app.register_listener(setup_clients, "before_server_start")
app.register_listener(setup_risk_manager, "before_server_start")
app.register_listener(setup_strategy_registry, "before_server_start")
app.register_listener(initialize_monitoring, "before_server_start")
app.register_listener(setup_background_tasks, "after_server_start")

app.register_listener(cleanup_background_tasks, "before_server_stop")
app.register_listener(cleanup_clients, "after_server_stop")

@app.after_server_stop
async def after_stop(app, loop):
    logger.info("TradingBotAPI server stopped")

if __name__ == "__main__":
    debug = os.getenv("DEBUG", "false").lower() == "true"
    workers = 1
    port = int(os.getenv("PORT", "8000"))
    logger.info("Starting TradingBotAPI with debug=%s, workers=%d, port=%d", debug, workers, port)
    app.run(
        host="0.0.0.0",
        port=port,
        debug=debug,
        auto_reload=debug,
        workers=workers,
        access_log=debug,
        single_process=True
    )