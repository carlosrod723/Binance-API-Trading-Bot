# app/api/orders.py

import time
import asyncio
from sanic import Blueprint
from sanic.request import Request
from sanic.response import json
from sanic_ext import openapi
from functools import wraps
from typing import Optional, Dict, Any, List, Union
from tenacity import retry, stop_after_attempt, wait_fixed
import logging

logger = logging.getLogger(__name__)

orders_bp = Blueprint("orders", url_prefix="/api/v1/order")

def validate_api_key(f):
    @wraps(f)
    async def decorated(request: Request, *args, **kwargs):
        api_key = request.headers.get("X-MBX-APIKEY")
        api_secret = request.headers.get("X-MBX-APISECRET")
        if not api_key or not api_secret:
            logger.warning("Request missing API key or secret")
            return json({"success": False, "error": "Missing API credentials"}, status=401)
        request.ctx.api_key = api_key
        request.ctx.api_secret = api_secret
        return await f(request, *args, **kwargs)
    return decorated

def validate_numeric_param(data: Dict[str, Any], field_name: str, required: bool = True) -> Dict[str, Any]:
    """
    Validate a numeric parameter from request data.
    
    Args:
        data: The request data dictionary
        field_name: The name of the field to validate
        required: Whether the field is required (default True)
    
    Returns:
        Dict with 'value' (the validated value or None) and 'error' (error message or None)
    """
    result = {'value': None, 'error': None}
    
    # If field doesn't exist
    if field_name not in data:
        if required:
            result['error'] = f"{field_name} is required"
        return result
        
    # If value is explicitly None
    if data[field_name] is None:
        if required:
            result['error'] = f"{field_name} cannot be null"
        return result
    
    # Try to convert to float
    try:
        value = float(data[field_name])
        if value <= 0:
            result['error'] = f"{field_name} must be a positive number"
        else:
            result['value'] = value
    except (TypeError, ValueError):
        result['error'] = f"{field_name} must be a valid number"
    
    return result

@orders_bp.route("/buy", methods=["POST"])
@openapi.tag("Orders")
@openapi.summary("Create a buy order")
@openapi.description("Creates a new limit buy order on Binance with optional stop-loss and take-profit.")
@openapi.parameter("X-MBX-APIKEY", str, "header", required=True)
@openapi.parameter("X-MBX-APISECRET", str, "header", required=True)
@openapi.body(
    {"application/json": {
        "coinPair": str,
        "quantity": float,
        "price": float,
        "stopLoss": Optional[float],
        "takeProfit": Optional[float]
    }},
    required=True
)
@openapi.response(200, {"application/json": {"success": bool, "orderId": str}})
@openapi.response(400, {"application/json": {"success": bool, "error": str}})
@openapi.response(401, {"application/json": {"success": bool, "error": str}})
@openapi.response(500, {"application/json": {"success": bool, "error": str}})
@validate_api_key
@retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
async def create_buy_order(request: Request):
    """Handle buy order creation on Binance."""
    data = request.json or {}
    errors = []
    
    # Validate coinPair
    coin_pair = data.get("coinPair")
    if not coin_pair:
        errors.append("coinPair is required")
    elif not isinstance(coin_pair, str):
        errors.append("coinPair must be a string")
    
    # Validate required numeric fields
    quantity_validation = validate_numeric_param(data, "quantity", required=True)
    price_validation = validate_numeric_param(data, "price", required=True)
    
    # Extract validation results
    quantity = quantity_validation['value']
    price = price_validation['value']
    
    # Collect errors
    if quantity_validation['error']:
        errors.append(quantity_validation['error'])
    if price_validation['error']:
        errors.append(price_validation['error'])
    
    # Validate optional fields
    stop_loss_validation = validate_numeric_param(data, "stopLoss", required=False)
    take_profit_validation = validate_numeric_param(data, "takeProfit", required=False)
    
    stop_loss = stop_loss_validation['value']
    take_profit = take_profit_validation['value']
    
    # Only add errors for optional fields if they were provided but invalid
    if stop_loss_validation['error'] and "stopLoss" in data:
        errors.append(stop_loss_validation['error'])
    if take_profit_validation['error'] and "takeProfit" in data:
        errors.append(take_profit_validation['error'])
    
    # Return if validation failed
    if errors:
        error_message = "; ".join(errors)
        logger.warning(f"Buy order validation failed: {error_message}")
        return json({"success": False, "error": error_message}, status=400)
    
    # If validation passed, proceed with order creation
    client = request.app.ctx.binance_client
    try:
        logger.info(f"Creating buy order for {coin_pair}, quantity={quantity}, price={price}, SL={stop_loss}, TP={take_profit}")
        order_id = await client.create_buy_order(coin_pair, quantity, price, stop_loss=stop_loss, take_profit=take_profit)
        
        if order_id:
            logger.info(f"Buy order created: {order_id} for {coin_pair}")
            return json({"success": True, "orderId": order_id})
        
        logger.error(f"Order creation failed for {coin_pair} with no error returned")
        return json({"success": False, "error": "Order creation failed"}, status=500)
        
    except Exception as e:
        logger.error(f"Failed to create buy order for {coin_pair}: {str(e)}")
        return json({"success": False, "error": f"Failed to create buy order: {str(e)}"}, status=500)

@orders_bp.route("/sell", methods=["POST"])
@openapi.tag("Orders")
@openapi.summary("Create a sell order")
@openapi.description("Creates a new limit sell order on Binance with optional stop-loss and take-profit.")
@openapi.parameter("X-MBX-APIKEY", str, "header", required=True)
@openapi.parameter("X-MBX-APISECRET", str, "header", required=True)
@openapi.body(
    {"application/json": {
        "coinPair": str,
        "quantity": float,
        "price": float,
        "stopLoss": Optional[float],
        "takeProfit": Optional[float]
    }},
    required=True
)
@openapi.response(200, {"application/json": {"success": bool, "orderId": str}})
@openapi.response(400, {"application/json": {"success": bool, "error": str}})
@openapi.response(401, {"application/json": {"success": bool, "error": str}})
@openapi.response(500, {"application/json": {"success": bool, "error": str}})
@validate_api_key
@retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
async def create_sell_order(request: Request):
    """Handle sell order creation on Binance."""
    data = request.json or {}
    errors = []
    
    # Validate coinPair
    coin_pair = data.get("coinPair")
    if not coin_pair:
        errors.append("coinPair is required")
    elif not isinstance(coin_pair, str):
        errors.append("coinPair must be a string")
    
    # Validate required numeric fields
    quantity_validation = validate_numeric_param(data, "quantity", required=True)
    price_validation = validate_numeric_param(data, "price", required=True)
    
    # Extract validation results
    quantity = quantity_validation['value']
    price = price_validation['value']
    
    # Collect errors
    if quantity_validation['error']:
        errors.append(quantity_validation['error'])
    if price_validation['error']:
        errors.append(price_validation['error'])
    
    # Validate optional fields
    stop_loss_validation = validate_numeric_param(data, "stopLoss", required=False)
    take_profit_validation = validate_numeric_param(data, "takeProfit", required=False)
    
    stop_loss = stop_loss_validation['value']
    take_profit = take_profit_validation['value']
    
    # Only add errors for optional fields if they were provided but invalid
    if stop_loss_validation['error'] and "stopLoss" in data:
        errors.append(stop_loss_validation['error'])
    if take_profit_validation['error'] and "takeProfit" in data:
        errors.append(take_profit_validation['error'])
    
    # Return if validation failed
    if errors:
        error_message = "; ".join(errors)
        logger.warning(f"Sell order validation failed: {error_message}")
        return json({"success": False, "error": error_message}, status=400)
    
    # If validation passed, proceed with order creation
    client = request.app.ctx.binance_client
    try:
        logger.info(f"Creating sell order for {coin_pair}, quantity={quantity}, price={price}, SL={stop_loss}, TP={take_profit}")
        order_id = await client.create_sell_order(coin_pair, quantity, price, stop_loss=stop_loss, take_profit=take_profit)
        
        if order_id:
            logger.info(f"Sell order created: {order_id} for {coin_pair}")
            return json({"success": True, "orderId": order_id})
        
        logger.error(f"Order creation failed for {coin_pair} with no error returned")
        return json({"success": False, "error": "Order creation failed"}, status=500)
        
    except Exception as e:
        logger.error(f"Failed to create sell order for {coin_pair}: {str(e)}")
        return json({"success": False, "error": f"Failed to create sell order: {str(e)}"}, status=500)

@orders_bp.route("/exit", methods=["POST"])
@openapi.tag("Orders")
@openapi.summary("Exit an existing position")
@openapi.description("Exits the entire position for the specified trading pair on Binance using a market order.")
@openapi.parameter("X-MBX-APIKEY", str, "header", required=True)
@openapi.parameter("X-MBX-APISECRET", str, "header", required=True)
@openapi.body({"application/json": {"coinPair": str}}, required=True)
@openapi.response(200, {"application/json": {"success": bool, "message": str}})
@openapi.response(400, {"application/json": {"success": bool, "error": str}})
@openapi.response(401, {"application/json": {"success": bool, "error": str}})
@openapi.response(500, {"application/json": {"success": bool, "error": str}})
@validate_api_key
@retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
async def exit_position(request: Request):
    """Handle position exit on Binance."""
    data = request.json or {}
    
    # Validate coinPair
    coin_pair = data.get("coinPair")
    if not coin_pair:
        logger.warning("Exit position request missing coinPair")
        return json({"success": False, "error": "coinPair is required"}, status=400)
    if not isinstance(coin_pair, str):
        logger.warning("Exit position request contains invalid coinPair type")
        return json({"success": False, "error": "coinPair must be a string"}, status=400)

    client = request.app.ctx.binance_client
    try:
        logger.info(f"Attempting to exit position for {coin_pair}")
        success = await client.exit_position(coin_pair)
        
        if success:
            logger.info(f"Position exited for {coin_pair}")
            return json({"success": True, "message": f"Position for {coin_pair} successfully exited"})
        
        logger.warning(f"No position to exit or exit failed for {coin_pair}")
        return json({"success": False, "error": f"No position to exit or exit failed for {coin_pair}"}, status=404)
        
    except Exception as e:
        logger.error(f"Failed to exit position for {coin_pair}: {str(e)}")
        return json({"success": False, "error": f"Failed to exit position: {str(e)}"}, status=500)
    
@orders_bp.route("/recent", methods=["GET"])
@openapi.tag("Orders")
@openapi.summary("Get recent orders")
@openapi.description("Retrieves recent orders/trades for a specified symbol from Binance.")
@openapi.parameter("X-MBX-APIKEY", str, "header", required=True)
@openapi.parameter("X-MBX-APISECRET", str, "header", required=True)
@openapi.parameter("symbol", str, "query", required=True, description="Trading pair symbol, e.g. BTCUSDT or 'all' for multiple")
@openapi.parameter("limit", int, "query", required=False, description="Maximum number of records to return (default: 50)")
@openapi.response(200, {"application/json": {"success": bool, "orders": list}})
@openapi.response(400, {"application/json": {"success": bool, "error": str}})
@openapi.response(401, {"application/json": {"success": bool, "error": str}})
@openapi.response(500, {"application/json": {"success": bool, "error": str}})
@validate_api_key
@retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
async def get_recent_orders(request: Request):
    """Handle requests to get recent orders for a specified symbol on Binance."""
    # Validate symbol
    symbol = request.args.get("symbol")
    if not symbol:
        logger.warning("Recent orders request missing symbol parameter")
        return json({"success": False, "error": "Symbol is required"}, status=400)
    
    # Special handling for 'all' symbols request from dashboard
    is_all_symbols = symbol.lower() == 'all'
    
    # Validate limit if provided
    limit = 50  # Default limit
    limit_str = request.args.get("limit")
    if limit_str:
        try:
            limit_val = int(limit_str)
            if limit_val > 0:
                limit = min(limit_val, 1000)  # Cap at 1000 to prevent abuse
            else:
                logger.warning(f"Invalid limit parameter: {limit_str}")
                return json({"success": False, "error": "Limit must be a positive integer"}, status=400)
        except ValueError:
            logger.warning(f"Non-integer limit parameter: {limit_str}")
            return json({"success": False, "error": "Limit must be a valid integer"}, status=400)

    client = request.app.ctx.binance_client
    if not client:
        logger.error("Binance client not initialized when retrieving recent orders")
        return json({"success": False, "error": "Trading service unavailable"}, status=503)
    
    try:
        # Check cache for 'all' symbol requests to reduce API calls
        cache_key = f"recent_orders_{symbol}_{limit}"
        if is_all_symbols and hasattr(request.app.ctx, 'api_cache') and 'orders' in request.app.ctx.api_cache:
            cache_entry = request.app.ctx.api_cache['orders']
            # Use cache if it's less than 10 seconds old
            if time.time() - cache_entry.get('timestamp', 0) < 10:
                logger.info(f"Using cached recent orders for 'all', age: {time.time() - cache_entry['timestamp']:.1f}s")
                return json(cache_entry.get('data', {"success": True, "orders": [], "count": 0}))
        
        # For 'all' symbols, get the most active trading pairs
        if is_all_symbols:
            logger.info(f"Getting recent orders for multiple active symbols (dashboard request)")
            # Use a smaller limit for each symbol to avoid rate limits
            per_symbol_limit = min(limit // 3, 10)
            
            # Try to get active symbols from cache or default to popular ones
            active_symbols = []
            if hasattr(request.app.ctx, 'valid_trading_pairs') and request.app.ctx.valid_trading_pairs['pairs']:
                # Use top traded pairs if available
                popular_pairs = ["BTCUSDT", "ETHUSDT", "BNBUSDT", "XRPUSDT", "ADAUSDT"]
                active_symbols = [s for s in popular_pairs if s in request.app.ctx.valid_trading_pairs['pairs']][:3]
            
            if not active_symbols:
                # Fallback to hardcoded popular pairs
                active_symbols = ["BTCUSDT", "ETHUSDT", "BNBUSDT"]
            
            # Fetch orders for each symbol
            all_orders = []
            for sym in active_symbols:
                try:
                    if hasattr(client, 'get_my_trades'):
                        orders = await client.get_my_trades(symbol=sym, limit=per_symbol_limit)
                    else:
                        orders = await client.get_account_trades(symbol=sym, limit=per_symbol_limit)
                    
                    all_orders.extend(orders)
                    # Small sleep to avoid rate limiting
                    await asyncio.sleep(0.2)
                except Exception as e:
                    logger.warning(f"Failed to fetch orders for {sym}: {str(e)}")
            
            # Sort by time (newest first)
            all_orders.sort(key=lambda x: x.get("time", 0), reverse=True)
            # Limit total orders
            all_orders = all_orders[:limit]
            
            logger.info(f"Retrieved {len(all_orders)} recent orders for multiple symbols")
            orders = all_orders
        else:
            logger.info(f"Fetching recent orders for {symbol}, limit={limit}")
            # Normal single symbol request
            if hasattr(client, 'get_my_trades'):
                orders = await client.get_my_trades(symbol=symbol, limit=limit)
            else:
                orders = await client.get_account_trades(symbol=symbol, limit=limit)
                
            logger.info(f"Retrieved {len(orders)} recent orders for {symbol}")
        
        # Ensure each order has consistent fields
        standardized_orders = []
        for order in orders:
            standardized_order = {
                "symbol": order.get("symbol"),
                "orderId": order.get("orderId"),
                "time": order.get("time"),
                "side": "BUY" if order.get("isBuyer", False) else "SELL",
                "price": float(order.get("price", 0)),
                "quantity": float(order.get("qty", 0)),
                "quoteQty": float(order.get("quoteQty", 0)),
                "commission": float(order.get("commission", 0)),
                "commissionAsset": order.get("commissionAsset", ""),
                "isBestMatch": order.get("isBestMatch", False)
            }
            standardized_orders.append(standardized_order)
        
        response_data = {
            "success": True, 
            "orders": standardized_orders,
            "count": len(standardized_orders)
        }
        
        # Cache the result for 'all' symbol requests
        if is_all_symbols and hasattr(request.app.ctx, 'api_cache'):
            request.app.ctx.api_cache['orders'] = {
                'data': response_data,
                'timestamp': time.time()
            }
            
        return json(response_data)
        
    except Exception as e:
        logger.error(f"Failed to fetch recent orders for {symbol}: {str(e)}")
        return json({"success": False, "error": f"Failed to fetch orders: {str(e)}"}, status=500)