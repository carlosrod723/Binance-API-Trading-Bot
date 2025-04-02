# app/api/strategy.py

from sanic import Blueprint
from sanic.request import Request
from sanic.response import json
from sanic_ext import openapi
from functools import wraps
import importlib
import os
import logging
import time
import traceback
from typing import Dict, Any, List, Optional, Type
from app.strategies.base_strategy import Strategy

logger = logging.getLogger(__name__)

strategy_bp = Blueprint("strategy", url_prefix="/api/v1/strategy")

def validate_api_key(f):
    @wraps(f)
    async def decorated(request: Request, *args, **kwargs):
        api_key = request.headers.get("X-MBX-APIKEY")
        api_secret = request.headers.get("X-MBX-APISECRET")
        if not api_key or not api_secret:
            logger.warning(f"[Request {id(request)}] Missing API key or secret")
            return json({"success": False, "error": "Missing API credentials"}, status=401)
        request.ctx.api_key = api_key
        request.ctx.api_secret = api_secret
        return await f(request, *args, **kwargs)
    return decorated

def standard_response(success: bool, data: Optional[Dict[str, Any]] = None, error: Optional[str] = None) -> Dict[str, Any]:
    """Create a standardized response format."""
    response = {"success": success, "timestamp": int(time.time() * 1000)}
    if data is not None:
        response.update(data)
    if error:
        response["error"] = error
    return response

async def setup_strategy_registry(app):
    """Cache available strategies and pandas-ta indicators with enhanced metadata."""
    try:
        # Initialize empty containers
        strategies = {}
        strategies_metadata = []
        
        # First, explicitly import known strategies to ensure they're loaded
        try:
            logger.info("Explicitly importing known strategies")
            
            # Import directly from specific modules
            from app.strategies.macd_strategy import MACDStrategy
            strategies['macd_strategy'] = MACDStrategy
            logger.info("Successfully imported MACDStrategy")
            
            from app.strategies.bollinger_reversal_strategy import BollingerReversalStrategy
            strategies['bollinger_reversal_strategy'] = BollingerReversalStrategy
            logger.info("Successfully imported BollingerReversalStrategy")
            
            from app.strategies.enhanced_macd_strategy import EnhancedMACDStrategy
            strategies['enhanced_macd_strategy'] = EnhancedMACDStrategy
            logger.info("Successfully imported EnhancedMACDStrategy")
            
            from app.strategies.volatility_breakout_strategy import VolatilityBreakoutStrategy
            strategies['volatility_breakout_strategy'] = VolatilityBreakoutStrategy
            logger.info("Successfully imported VolatilityBreakoutStrategy")
            
            # Add any other known strategies here
            
        except ImportError as e:
            logger.warning(f"Error importing known strategies: {str(e)}")
            logger.debug(f"Import error traceback: {traceback.format_exc()}")
        
        # Then try dynamic discovery for other strategies
        try:
            strategies_dir = getattr(app.ctx.config, 'STRATEGIES_DIR', None)
            
            # Fallback to default directory if not configured
            if not strategies_dir:
                # Use the already imported os module from line 9
                import os as _os  # Import with a different name to avoid confusion
                strategies_dir = _os.path.join(_os.path.dirname(_os.path.dirname(__file__)), 'strategies')
                logger.warning(f"STRATEGIES_DIR not configured, using default: {strategies_dir}")
            
            if not os.path.exists(strategies_dir):
                logger.error(f"Strategies directory not found: {strategies_dir}")
            else:
                logger.info(f"Loading strategies from {strategies_dir}")
                
                for filename in os.listdir(strategies_dir):
                    # Skip already imported strategies
                    strategy_name = filename[:-3] if filename.endswith(".py") else None
                    if (strategy_name and 
                        strategy_name not in ["__init__", "base_strategy"] and 
                        strategy_name not in strategies and 
                        filename.endswith(".py")):
                        
                        try:
                            module = importlib.import_module(f"app.strategies.{strategy_name}")
                            strategy_class = None
                            
                            for name, obj in module.__dict__.items():
                                if isinstance(obj, type) and issubclass(obj, Strategy) and obj is not Strategy:
                                    strategy_class = obj
                                    strategies[strategy_name] = obj
                                    logger.info(f"Dynamically loaded strategy: {strategy_name} ({name})")
                                    break
                                    
                            if not strategy_class:
                                logger.warning(f"No valid strategy class found in {strategy_name}.py")
                                
                        except Exception as e:
                            logger.error(f"Failed to load strategy {strategy_name}: {str(e)}")
                            logger.debug(f"Exception traceback: {traceback.format_exc()}")
                    elif strategy_name in strategies:
                        logger.debug(f"Skipping already loaded strategy: {strategy_name}")
                
        except Exception as e:
            logger.error(f"Error in dynamic strategy discovery: {str(e)}")
            logger.debug(f"Exception traceback: {traceback.format_exc()}")
        
        # Build metadata for all strategies (from both explicit and dynamic loading)
        for strategy_name, strategy_class in strategies.items():
            try:
                # Get strategy parameters with defaults and descriptions
                param_metadata = []
                
                # Check if the strategy has a params_info method or attribute
                if hasattr(strategy_class, 'get_parameters_info') and callable(getattr(strategy_class, 'get_parameters_info')):
                    params_info = strategy_class.get_parameters_info()
                else:
                    # Attempt to extract parameter info from docstring or init signature
                    import inspect
                    params_info = {}
                    
                    # Get parameter information from constructor
                    signature = inspect.signature(strategy_class.__init__)
                    for param_name, param in signature.parameters.items():
                        if param_name not in ('self', 'args', 'kwargs'):
                            param_info = {
                                'name': param_name,
                                'type': 'number',  # Default assumption
                                'required': param.default is inspect.Parameter.empty,
                                'default': None if param.default is inspect.Parameter.empty else param.default
                            }
                            params_info[param_name] = param_info
                
                # Format params for metadata
                for param_name, param_info in params_info.items():
                    param_metadata.append({
                        'name': param_name,
                        'type': param_info.get('type', 'number'),
                        'required': param_info.get('required', True),
                        'default': param_info.get('default'),
                        'min': param_info.get('min'),
                        'max': param_info.get('max'),
                        'description': param_info.get('description', f"{param_name} parameter")
                    })
                
                # Get strategy description
                description = (strategy_class.__doc__ or "").strip() or f"{strategy_name} trading strategy"
                
                # Format strategy name for display (remove _strategy suffix, convert underscores to spaces)
                display_name = strategy_name
                if display_name.endswith('_strategy'):
                    display_name = display_name[:-9]  # Remove '_strategy' suffix
                display_name = display_name.replace('_', ' ').title()
                
                # Add strategy metadata
                strategies_metadata.append({
                    'id': strategy_name,
                    'name': getattr(strategy_class, 'STRATEGY_NAME', display_name),
                    'description': description,
                    'parameters': param_metadata,
                    'type': getattr(strategy_class, 'STRATEGY_TYPE', 'Technical'),
                    'risk_level': getattr(strategy_class, 'RISK_LEVEL', 'medium'),
                    'time_frame': getattr(strategy_class, 'TIME_FRAME', 'medium'),
                    'version': getattr(strategy_class, 'VERSION', '1.0')
                })
                
                logger.info(f"Added metadata for strategy: {strategy_name}")
                
            except Exception as e:
                logger.error(f"Failed to create metadata for strategy {strategy_name}: {str(e)}")
                logger.debug(f"Exception traceback: {traceback.format_exc()}")
        
        # Store strategies and metadata in app context
        app.ctx.strategies = strategies
        app.ctx.strategies_metadata = strategies_metadata
        logger.info(f"Loaded {len(strategies)} strategies: {list(strategies.keys())}")

        # Load pandas-ta indicators with improved metadata
        try:
            import pandas_ta as ta
            pandas_ta_indicators = []
            
            # Define indicator groups with metadata
            indicator_groups = {
                'momentum': {
                    'display_name': 'Momentum',
                    'indicators': ['ao', 'apo', 'bias', 'bop', 'brar', 'cci', 'cfo', 'cg', 'cmo', 
                                'cti', 'er', 'fisher', 'inertia', 'kdj', 'kst', 'macd', 'mom', 
                                'pgo', 'ppo', 'psl', 'pvo', 'qqe', 'roc', 'rsi', 'rsx', 'rvgi', 
                                'slope', 'smi', 'squeeze', 'stoch', 'stochrsi', 'trix', 'tsi', 
                                'uo', 'willr'],
                    'inputs': ["close"]
                },
                'trend': {
                    'display_name': 'Trend',
                    'indicators': ['adx', 'aroon', 'chop', 'cksp', 'decay', 'decreasing', 'dpo', 
                                'increasing', 'linear_decay', 'long_run', 'psar', 'qstick', 
                                'short_run', 'supertrend', 'vhf', 'vortex'],
                    'inputs': ["close", "high", "low"]
                },
                'volatility': {
                    'display_name': 'Volatility',
                    'indicators': ['aberration', 'accbands', 'atr', 'bbands', 'donchian', 'hwc', 
                                'kc', 'massi', 'natr', 'pdist', 'rvi', 'true_range'],
                    'inputs': ["close", "high", "low"]
                },
                'volume': {
                    'display_name': 'Volume',
                    'indicators': ['ad', 'adosc', 'aobv', 'cmf', 'efi', 'eom', 'mfi', 'nvi', 
                                'obv', 'pvi', 'pvol', 'pvt'],
                    'inputs': ["close", "volume"]
                },
                'moving_average': {
                    'display_name': 'Moving Averages',
                    'indicators': ['dema', 'ema', 'fwma', 'hilo', 'hl2', 'hlc3', 'hma', 'kama', 
                                'linreg', 'ma', 'mcgd', 'midpoint', 'midprice', 'ohlc4', 'pwma', 
                                'rma', 'sinwma', 'sma', 'ssf', 't3', 'tema', 'trima', 'vidya', 
                                'vwap', 'vwma', 'wcp', 'wma', 'zlma'],
                    'inputs': ["close"]
                }
            }
            
            # Process each group and create indicator metadata
            for group_key, group_info in indicator_groups.items():
                for func_name in group_info['indicators']:
                    if hasattr(ta, func_name):
                        # Get function signature to determine parameters
                        import inspect
                        try:
                            func = getattr(ta, func_name)
                            signature = inspect.signature(func)
                            params = {}
                            
                            for param_name, param in signature.parameters.items():
                                if param_name not in ('kwargs'):
                                    params[param_name] = {
                                        'default': None if param.default is inspect.Parameter.empty else param.default,
                                        'required': param.default is inspect.Parameter.empty
                                    }
                            
                            # Create indicator metadata
                            indicator_data = {
                                "name": func_name,
                                "display_name": func_name.upper(),
                                "group": group_key,
                                "group_display": group_info['display_name'],
                                "inputs": group_info['inputs'],
                                "parameters": params,
                                "outputs": [func_name]
                            }
                            pandas_ta_indicators.append(indicator_data)
                        except Exception as e:
                            logger.warning(f"Failed to analyze indicator {func_name}: {str(e)}")
            
            app.ctx.indicators = pandas_ta_indicators
            logger.info(f"Loaded {len(pandas_ta_indicators)} pandas-ta indicators")
        except ImportError:
            logger.warning("pandas-ta is not available")
            app.ctx.indicators = []
            
    except Exception as e:
        logger.error(f"Error in setup_strategy_registry: {str(e)}")
        logger.debug(f"Exception traceback: {traceback.format_exc()}")
        app.ctx.strategies = {}
        app.ctx.strategies_metadata = []
        app.ctx.indicators = []

async def load_strategy(app, strategy_name: str, params: Optional[Dict[str, Any]] = None) -> Optional[Strategy]:
    """Load and instantiate a strategy with parameter validation."""
    try:
        strategy_class = app.ctx.strategies.get(strategy_name)
        if not strategy_class:
            logger.error(f"Strategy '{strategy_name}' not found")
            return None
        
        # Get parameter metadata
        strategy_metadata = next((s for s in app.ctx.strategies_metadata if s['id'] == strategy_name), None)
        params_metadata = strategy_metadata['parameters'] if strategy_metadata else []
        
        # Initialize with default parameters
        validated_params = {}
        for param_info in params_metadata:
            param_name = param_info['name']
            default_value = param_info.get('default')
            
            if params and param_name in params:
                param_value = params[param_name]
                
                # Type validation and conversion
                if param_info.get('type') == 'number':
                    try:
                        param_value = float(param_value)
                        
                        # Range validation
                        if 'min' in param_info and param_value < param_info['min']:
                            logger.warning(f"Parameter {param_name} value {param_value} is below minimum {param_info['min']}")
                            param_value = param_info['min']
                            
                        if 'max' in param_info and param_value > param_info['max']:
                            logger.warning(f"Parameter {param_name} value {param_value} is above maximum {param_info['max']}")
                            param_value = param_info['max']
                            
                    except (TypeError, ValueError):
                        logger.warning(f"Invalid numeric value for parameter {param_name}: {param_value}")
                        param_value = default_value
                        
                elif param_info.get('type') == 'boolean' and not isinstance(param_value, bool):
                    param_value = param_value in (True, 'true', 'True', '1', 1)
                    
                elif param_info.get('type') == 'string' and not isinstance(param_value, str):
                    param_value = str(param_value)
                
                validated_params[param_name] = param_value
            elif param_info.get('required', False) and default_value is None:
                logger.error(f"Required parameter {param_name} is missing")
                return None
            else:
                # Use default value for missing parameters
                validated_params[param_name] = default_value
        
        # Create strategy instance with validated parameters
        strategy = strategy_class(**validated_params)
        
        # Store in app context
        app.ctx.active_strategy = strategy
        app.ctx.active_strategy_name = strategy_name
        app.ctx.strategy_params = validated_params
        
        logger.info(f"Loaded strategy: {strategy_name} with parameters: {validated_params}")
        return strategy
        
    except Exception as e:
        logger.error(f"Failed to instantiate strategy {strategy_name}: {str(e)}")
        logger.debug(f"Exception traceback: {traceback.format_exc()}")
        app.ctx.active_strategy = None
        app.ctx.active_strategy_name = None
        app.ctx.strategy_params = {}
        return None

@strategy_bp.route("/", methods=["POST"])
@openapi.tag("Strategy")
@openapi.summary("Select and load a trading strategy")
@openapi.description("Selects and loads the specified trading strategy for use with Binance.")
@openapi.parameter("X-MBX-APIKEY", str, "header", required=True, description="Binance API Key")
@openapi.parameter("X-MBX-APISECRET", str, "header", required=True, description="Binance API Secret")
@openapi.body({"application/json": {"strategyName": str, "parameters": dict}}, required=True)
@openapi.response(200, {"application/json": {"success": bool, "message": str, "strategyName": str, "parameters": dict}})
@openapi.response(400, {"application/json": {"success": bool, "error": str}})
@openapi.response(401, {"application/json": {"success": bool, "error": str}})
@openapi.response(404, {"application/json": {"success": bool, "error": str}})
@openapi.response(500, {"application/json": {"success": bool, "error": str}})
@validate_api_key
async def select_strategy(request: Request):
    """Handle strategy selection with parameter validation."""
    request_id = id(request)
    logger.info(f"[{request_id}] Processing strategy selection request")
    
    try:
        data = request.json or {}
        
        # Validate strategy name
        strategy_name = data.get("strategyName")
        if not isinstance(strategy_name, str) or not strategy_name:
            logger.warning(f"[{request_id}] Invalid strategyName: {strategy_name}")
            return json(
                standard_response(False, error="Invalid strategyName, must be a non-empty string"), 
                status=400
            )
            
        # Check if strategy exists
        if strategy_name not in request.app.ctx.strategies:
            logger.warning(f"[{request_id}] Strategy '{strategy_name}' not found")
            return json(
                standard_response(False, error=f"Strategy '{strategy_name}' not found"), 
                status=404
            )
        
        # Get optional parameters
        parameters = data.get("parameters", {})
        if parameters is not None and not isinstance(parameters, dict):
            logger.warning(f"[{request_id}] Invalid parameters: {parameters}")
            return json(
                standard_response(False, error="Parameters must be a dictionary or null"), 
                status=400
            )
        
        # Load strategy
        strategy = await load_strategy(request.app, strategy_name, parameters)
        if strategy:
            # Return success with strategy info
            strategy_params = request.app.ctx.strategy_params
            
            return json(
                standard_response(True, {
                    "message": f"Strategy '{strategy_name}' loaded successfully",
                    "strategyName": strategy_name,
                    "parameters": strategy_params
                })
            )
            
        # Strategy loading failed
        return json(
            standard_response(False, error=f"Failed to load strategy '{strategy_name}'"), 
            status=500
        )
            
    except Exception as e:
        logger.error(f"[{request_id}] Error selecting strategy: {str(e)}")
        logger.debug(f"[{request_id}] Exception traceback: {traceback.format_exc()}")
        return json(
            standard_response(False, error=f"Server error: {str(e)}"), 
            status=500
        )

@strategy_bp.route("/parameters", methods=["POST"])
@openapi.tag("Strategy")
@openapi.summary("Set parameters for the active strategy")
@openapi.description("Sets parameters for the currently active trading strategy on Binance.")
@openapi.parameter("X-MBX-APIKEY", str, "header", required=True, description="Binance API Key")
@openapi.parameter("X-MBX-APISECRET", str, "header", required=True, description="Binance API Secret")
@openapi.body({"application/json": {"strategyName": str, "parameters": dict}}, required=True)
@openapi.response(200, {"application/json": {"success": bool, "message": str, "strategyName": str, "parameters": dict}})
@openapi.response(400, {"application/json": {"success": bool, "error": str}})
@openapi.response(401, {"application/json": {"success": bool, "error": str}})
@openapi.response(404, {"application/json": {"success": bool, "error": str}})
@openapi.response(500, {"application/json": {"success": bool, "error": str}})
@validate_api_key
async def set_strategy_parameters(request: Request):
    """Handle setting strategy parameters with validation."""
    request_id = id(request)
    logger.info(f"[{request_id}] Processing strategy parameter update request")
    
    try:
        data = request.json or {}
        
        # Validate strategy name
        strategy_name = data.get("strategyName")
        if not isinstance(strategy_name, str) or not strategy_name:
            logger.warning(f"[{request_id}] Invalid strategyName: {strategy_name}")
            return json(
                standard_response(False, error="Invalid strategyName, must be a non-empty string"), 
                status=400
            )
        
        # Validate parameters
        parameters = data.get("parameters")
        if not isinstance(parameters, dict):
            logger.warning(f"[{request_id}] Invalid parameters: {parameters}")
            return json(
                standard_response(False, error="Parameters must be a dictionary"), 
                status=400
            )
        
        # Check if strategy is active
        active_strategy_name = getattr(request.app.ctx, "active_strategy_name", None)
        if active_strategy_name != strategy_name:
            logger.warning(f"[{request_id}] Strategy '{strategy_name}' is not active (current: {active_strategy_name})")
            return json(
                standard_response(False, error=f"Strategy '{strategy_name}' is not active"), 
                status=404
            )
        
        # Update strategy with new parameters
        strategy = await load_strategy(request.app, strategy_name, parameters)
        if strategy:
            # Return success with updated parameters
            strategy_params = request.app.ctx.strategy_params
            
            return json(
                standard_response(True, {
                    "message": f"Parameters for strategy '{strategy_name}' updated successfully",
                    "strategyName": strategy_name,
                    "parameters": strategy_params
                })
            )
            
        # Strategy update failed
        return json(
            standard_response(False, error=f"Failed to update parameters for strategy '{strategy_name}'"), 
            status=500
        )
            
    except Exception as e:
        logger.error(f"[{request_id}] Error updating strategy parameters: {str(e)}")
        logger.debug(f"[{request_id}] Exception traceback: {traceback.format_exc()}")
        return json(
            standard_response(False, error=f"Server error: {str(e)}"), 
            status=500
        )

@strategy_bp.get("/list")
@openapi.tag("Strategy")
@openapi.summary("List available strategies")
@openapi.description("Lists all available trading strategies for Binance with metadata about each strategy.")
@openapi.parameter("X-MBX-APIKEY", str, "header", required=True, description="Binance API Key")
@openapi.parameter("X-MBX-APISECRET", str, "header", required=True, description="Binance API Secret")
@openapi.parameter("detailed", bool, "query", required=False, description="Include detailed metadata about each strategy")
@openapi.response(200, {"application/json": {"success": bool, "strategies": list, "count": int}})
@openapi.response(401, {"application/json": {"success": bool, "error": str}})
@openapi.response(500, {"application/json": {"success": bool, "error": str}})
@validate_api_key
async def list_strategies(request: Request):
    """Handle listing available strategies with metadata."""
    request_id = id(request)
    logger.info(f"[{request_id}] Processing strategy list request")
    
    try:
        # Check if detailed info is requested
        detailed = request.args.get("detailed", "false").lower() in ("true", "1", "yes")
        
        if detailed:
            # Return full metadata
            strategies = request.app.ctx.strategies_metadata
        else:
            # Return simplified list
            strategies = [
                {
                    "id": s["id"],
                    "name": s["name"],
                    "description": s["description"],
                    "type": s["type"],
                    "risk_level": s["risk_level"]
                }
                for s in request.app.ctx.strategies_metadata
            ]
        
        # Add active strategy information
        active_strategy = getattr(request.app.ctx, "active_strategy_name", None)
        
        return json(
            standard_response(True, {
                "strategies": strategies,
                "count": len(strategies),
                "activeStrategy": active_strategy
            })
        )
        
    except Exception as e:
        logger.error(f"[{request_id}] Error listing strategies: {str(e)}")
        logger.debug(f"[{request_id}] Exception traceback: {traceback.format_exc()}")
        return json(
            standard_response(False, error=f"Server error: {str(e)}"), 
            status=500
        )

@strategy_bp.get("/indicators")
@openapi.tag("Strategy")
@openapi.summary("List available technical indicators")
@openapi.description("Lists all pandas-ta indicators available for strategy building, optionally filtered by group.")
@openapi.parameter("X-MBX-APIKEY", str, "header", required=True, description="Binance API Key")
@openapi.parameter("X-MBX-APISECRET", str, "header", required=True, description="Binance API Secret")
@openapi.parameter("group", str, "query", required=False, description="Filter indicators by group (momentum, trend, volatility, volume, moving_average)")
@openapi.response(200, {"application/json": {"success": bool, "indicators": list, "count": int, "groups": list}})
@openapi.response(400, {"application/json": {"success": bool, "error": str}})
@openapi.response(401, {"application/json": {"success": bool, "error": str}})
@openapi.response(500, {"application/json": {"success": bool, "error": str}})
@validate_api_key
async def list_indicators(request: Request):
    """Handle listing pandas-ta indicators with filtering."""
    request_id = id(request)
    logger.info(f"[{request_id}] Processing indicators list request")
    
    try:
        # Get optional group filter
        group = request.args.get("group")
        
        # Get indicators from app context
        indicators = request.app.ctx.indicators
        
        # Apply group filter if specified
        if group:
            valid_groups = ["momentum", "trend", "volatility", "volume", "moving_average"]
            if group not in valid_groups:
                return json(
                    standard_response(False, error=f"Invalid group: {group}. Valid groups: {', '.join(valid_groups)}"), 
                    status=400
                )
                
            indicators = [i for i in indicators if i.get("group") == group]
        
        # Count indicators by group for summary
        group_counts = {}
        for indicator in request.app.ctx.indicators:
            group_name = indicator.get("group", "other")
            group_display = indicator.get("group_display", group_name.capitalize())
            
            if group_name not in group_counts:
                group_counts[group_name] = {
                    "name": group_name,
                    "display_name": group_display,
                    "count": 0
                }
                
            group_counts[group_name]["count"] += 1
        
        return json(
            standard_response(True, {
                "indicators": indicators,
                "count": len(indicators),
                "groups": list(group_counts.values())
            })
        )
        
    except Exception as e:
        logger.error(f"[{request_id}] Error listing indicators: {str(e)}")
        logger.debug(f"[{request_id}] Exception traceback: {traceback.format_exc()}")
        return json(
            standard_response(False, error=f"Server error: {str(e)}"), 
            status=500
        )

@strategy_bp.get("/current")
@openapi.tag("Strategy")
@openapi.summary("Get current strategy and parameters")
@openapi.description("Retrieves the currently active strategy and its parameters on Binance.")
@openapi.parameter("X-MBX-APIKEY", str, "header", required=True, description="Binance API Key")
@openapi.parameter("X-MBX-APISECRET", str, "header", required=True, description="Binance API Secret")
@openapi.response(200, {"application/json": {"success": bool, "strategyName": str, "parameters": dict, "metadata": dict}})
@openapi.response(401, {"application/json": {"success": bool, "error": str}})
@openapi.response(404, {"application/json": {"success": bool, "error": str}})
@validate_api_key
async def get_current_strategy(request: Request):
    """Handle retrieving the current strategy with enhanced metadata."""
    request_id = id(request)
    logger.info(f"[{request_id}] Processing current strategy request")
    
    try:
        # Check if a strategy is active
        active_strategy_name = getattr(request.app.ctx, "active_strategy_name", None)
        if not active_strategy_name:
            logger.warning(f"[{request_id}] No active strategy")
            # Return a 200 response with a flag indicating no active strategy
            # This avoids triggering error handling in the frontend
            return json(
                standard_response(True, {
                    "strategyName": None,
                    "parameters": {},
                    "metadata": {},
                    "active": False,
                    "message": "No active strategy"
                })
            )
            
        # Get strategy parameters and metadata
        strategy_params = getattr(request.app.ctx, "strategy_params", {})
        strategy_metadata = next((s for s in request.app.ctx.strategies_metadata if s['id'] == active_strategy_name), {})
        
        return json(
            standard_response(True, {
                "strategyName": active_strategy_name,
                "parameters": strategy_params,
                "metadata": strategy_metadata,
                "active": True
            })
        )
        
    except Exception as e:
        logger.error(f"[{request_id}] Error getting current strategy: {str(e)}")
        logger.debug(f"[{request_id}] Exception traceback: {traceback.format_exc()}")
        return json(
            standard_response(False, error=f"Server error: {str(e)}"), 
            status=500
        )