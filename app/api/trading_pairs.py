from sanic import Blueprint
from sanic.request import Request
from sanic.response import json
from sanic_ext import openapi
from functools import wraps
import asyncio  # Added this import
import time
import logging
import traceback
from typing import Dict, List, Any, Optional

logger = logging.getLogger(__name__)

trading_pairs_bp = Blueprint("trading_pairs", url_prefix="/api/v1/trading")

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

@trading_pairs_bp.get("/pairs")
@openapi.tag("Trading")
@openapi.summary("Get available trading pairs")
@openapi.description("Retrieves all available trading pairs from Binance.")
@openapi.parameter("X-MBX-APIKEY", str, "header", required=True, description="Binance API Key")
@openapi.parameter("X-MBX-APISECRET", str, "header", required=True, description="Binance API Secret")
@openapi.parameter("quoteAsset", str, "query", required=False, description="Filter by quote asset (e.g., USDT)")
@openapi.parameter("limit", int, "query", required=False, description="Maximum number of pairs to return")
@openapi.response(200, {"application/json": {"success": bool, "pairs": List[Dict[str, Any]], "count": int}})
@openapi.response(401, {"application/json": {"success": bool, "error": str}})
@openapi.response(500, {"application/json": {"success": bool, "error": str}})
@validate_api_key
async def get_trading_pairs(request: Request):
    """Get available trading pairs with current prices."""
    request_id = id(request)
    logger.info(f"[{request_id}] Processing trading pairs request")
    
    # Parse query parameters
    quote_asset = request.args.get("quoteAsset", "USDT")
    
    limit_str = request.args.get("limit", "100")
    try:
        limit = int(limit_str)
        if limit <= 0:
            limit = 100
        elif limit > 1000:
            limit = 1000
    except ValueError:
        limit = 100
    
    client = request.app.ctx.binance_client
    
    try:
        # Get exchange information
        logger.info(f"[{request_id}] Fetching exchange information")
        
        # Get the underlying binance client
        if not client.initialized or not client.client:
            await client.initialize()
        
        binance_client = client.client
        
        # Get exchange info
        exchange_info = await binance_client.get_exchange_info()
        
        if not exchange_info or "symbols" not in exchange_info:
            logger.error(f"[{request_id}] Invalid exchange info from Binance")
            return json({
                "success": False,
                "timestamp": int(time.time() * 1000),
                "error": "Failed to retrieve exchange information"
            }, status=500)
        
        # Filter symbols by quote asset
        symbols = [
            s for s in exchange_info["symbols"]
            if s.get("quoteAsset") == quote_asset and s.get("status") == "TRADING"
        ]
        
        # Limit number of symbols
        symbols = symbols[:limit]
        
        # Get current prices for each symbol with limited concurrency
        logger.info(f"[{request_id}] Fetching current prices for {len(symbols)} symbols")
        
        async def get_symbol_price(symbol_info):
            symbol = symbol_info["symbol"]
            try:
                ticker = await binance_client.get_symbol_ticker(symbol=symbol)
                price = float(ticker["price"]) if ticker and "price" in ticker else 0.0
                return {**symbol_info, "price": price}
            except Exception as e:
                logger.warning(f"[{request_id}] Error getting price for {symbol}: {str(e)}")
                return {**symbol_info, "price": 0.0}
        
        # Process symbols in batches to avoid rate limiting
        batch_size = 5
        result_pairs = []
        
        for i in range(0, len(symbols), batch_size):
            batch = symbols[i:i+batch_size]
            batch_tasks = [get_symbol_price(s) for s in batch]
            batch_results = await asyncio.gather(*batch_tasks, return_exceptions=True)
            
            for result in batch_results:
                if isinstance(result, Exception):
                    logger.warning(f"[{request_id}] Error in batch: {str(result)}")
                    continue
                
                # Format pair info
                pair_info = {
                    "symbol": result["symbol"],
                    "baseAsset": result["baseAsset"],
                    "quoteAsset": result["quoteAsset"],
                    "price": result.get("price", 0.0),
                    "status": result["status"],
                    "isSpotTradingAllowed": result.get("isSpotTradingAllowed", True),
                    "filters": result.get("filters", [])
                }
                result_pairs.append(pair_info)
            
            # Short delay between batches
            await asyncio.sleep(0.2)
        
        # Sort pairs by base asset
        result_pairs.sort(key=lambda x: x["baseAsset"])
        
        logger.info(f"[{request_id}] Successfully retrieved {len(result_pairs)} trading pairs")
        return json({
            "success": True,
            "timestamp": int(time.time() * 1000),
            "pairs": result_pairs,
            "count": len(result_pairs),
            "quoteAsset": quote_asset
        })
        
    except Exception as e:
        logger.error(f"[{request_id}] Error fetching trading pairs: {str(e)}")
        logger.error(f"[{request_id}] Exception traceback: {traceback.format_exc()}")
        return json({
            "success": False,
            "timestamp": int(time.time() * 1000),
            "error": str(e)
        }, status=500)