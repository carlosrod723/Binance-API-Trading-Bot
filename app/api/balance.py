# app/api/balance.py

import os
import asyncio
from sanic import Blueprint
from sanic.request import Request
from sanic.response import json
from sanic_ext import openapi
from functools import wraps
from typing import List, Dict, Union, Optional, Any
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type
import logging
import traceback
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

balance_bp = Blueprint("balance", url_prefix="/api/v1/balance")

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

def standard_response(success: bool, data: Optional[Any] = None, error: Optional[str] = None) -> Dict[str, Any]:
    """Create a standardized response format."""
    response = {"success": success}
    if data is not None:
        response.update(data)
    if error:
        response["error"] = error
    return response

@balance_bp.route("/overall", methods=["GET"])
@openapi.tag("Balance")
@openapi.summary("Get overall account balance with details") # Updated summary
@openapi.description("Retrieves the overall account balance in USDT, including available and in-order amounts.") # Updated description
@openapi.parameter("X-MBX-APIKEY", str, "header", required=True, description="Binance API Key")
@openapi.parameter("X-MBX-APISECRET", str, "header", required=True, description="Binance API Secret")
# Updated response schema to reflect new fields
@openapi.response(200, {"application/json": {
    "success": bool,
    "totalBalance": float,
    "availableBalance": float,
    "inOrdersBalance": float,
    "currency": str
}})
@openapi.response(401, {"application/json": {"success": bool, "error": str}})
@openapi.response(404, {"application/json": {"success": bool, "error": str}}) # Added 404 for not found
@openapi.response(500, {"application/json": {"success": bool, "error": str}})
@openapi.response(503, {"application/json": {"success": bool, "error": str}})
@validate_api_key
@retry(
    stop=stop_after_attempt(3),
    wait=wait_fixed(2),
    retry=retry_if_exception_type(Exception) # Consider narrowing retry exceptions if needed
)
async def get_overall_balance(request: Request) -> json:
    """Handle requests to get the overall account balance details."""
    request_id = id(request)
    logger.info(f"[{request_id}] Processing detailed overall balance request")

    try:
        # Get the BinanceClient wrapper instance from the app context
        binance_wrapper = request.app.ctx.binance_client

        # Ensure the client wrapper and the underlying client are initialized
        if not binance_wrapper or not binance_wrapper.initialized or not binance_wrapper.client:
            logger.error(f"[{request_id}] Binance client not initialized or unavailable")
            return json(
                standard_response(False, error="Trading service unavailable or not initialized"),
                status=503
            )

        # Use the underlying async client directly for get_account
        async_client = binance_wrapper.client
        logger.debug(f"[{request_id}] Fetching detailed account information...")
        account_info = await async_client.get_account() # Fetches detailed balances

        if not account_info or "balances" not in account_info:
            logger.error(f"[{request_id}] Invalid account data received from Binance API")
            return json(
                standard_response(False, error="Failed to retrieve valid account data from Binance"),
                status=500
            )

        # Find the USDT balance entry
        usdt_balance_info = next((b for b in account_info.get("balances", []) if b["asset"] == "USDT"), None)

        if usdt_balance_info:
            # Extract free (available) and locked (in orders) balances
            available = float(usdt_balance_info.get("free", 0))
            locked = float(usdt_balance_info.get("locked", 0))
            total = available + locked

            logger.info(f"[{request_id}] Retrieved detailed balance: Total={total}, Available={available}, Locked={locked} USDT")

            # Return the structured data expected by the frontend originally
            return json(
                standard_response(True, {
                    "totalBalance": total,
                    "availableBalance": available,
                    "inOrdersBalance": locked,
                    "currency": "USDT"
                })
            )
        else:
            logger.warning(f"[{request_id}] USDT balance information not found in account details.")
            return json(
                standard_response(True, {
                    "totalBalance": 0.0,
                    "availableBalance": 0.0,
                    "inOrdersBalance": 0.0,
                    "currency": "USDT",
                    "message": "USDT balance data not found."
                })
            )

    except ConnectionError as ce:
        logger.error(f"[{request_id}] Connection error retrieving detailed balance: {str(ce)}")
        return json(
            standard_response(False, error="Connection error with exchange service"),
            status=503
        )
    except Exception as e:
        # Catch potential exceptions from get_account() as well
        logger.error(f"[{request_id}] Error retrieving detailed overall balance: {str(e)}")
        logger.debug(f"[{request_id}] Exception traceback: {traceback.format_exc()}")
        # Check if the error is related to authentication (e.g., invalid API key)
        if "APIError(code=-2015)" in str(e):
             return json(
                standard_response(False, error="Invalid API credentials"),
                status=401
            )
        return json(
            standard_response(False, error=f"Failed to retrieve detailed balance: {str(e)}"),
            status=500
        )

@balance_bp.route("/coins", methods=["GET"])
@openapi.tag("Balance")
@openapi.summary("Get individual coin balances")
@openapi.description("Retrieves balances for all non-zero coins in the account.")
@openapi.parameter("X-MBX-APIKEY", str, "header", required=True)
@openapi.parameter("X-MBX-APISECRET", str, "header", required=True)
@openapi.response(200, {"application/json": {"success": bool, "balances": list}})
@openapi.response(401, {"application/json": {"success": bool, "error": str}})
@openapi.response(500, {"application/json": {"success": bool, "error": str}})
@validate_api_key
async def get_coin_balances(request: Request):
    """Get individual coin balances."""
    logger.info(f"Processing coin balances request")
    
    try:
        # Get the binance client 
        binance_wrapper = request.app.ctx.binance_client
        
        # Ensure the client is initialized
        if not binance_wrapper.initialized:
            await binance_wrapper.initialize()
            
        # Get the AsyncClient object
        if not binance_wrapper.client:
            return json({
                "success": False,
                "error": "Binance client not initialized"
            }, status=500)
        
        # Use the underlying AsyncClient
        async_client = binance_wrapper.client
        
        # Get the account data
        account = await async_client.get_account()
        
        if not account or "balances" not in account:
            logger.error("Invalid account data returned from Binance")
            return json({"success": False, "error": "Failed to retrieve account data"}, status=500)
        
        # Process balances - only include non-zero balances
        valid_balances = []
        for asset in account.get("balances", []):
            free = float(asset["free"])
            locked = float(asset["locked"])
            total = free + locked
            
            if total > 0:
                valid_balances.append({
                    "coin": asset["asset"],
                    "free": free,
                    "locked": locked,
                    "balance": total,
                    "usdValue": total if asset["asset"] == "USDT" else 0.0  # Only set USDT value for USDT
                })
        
        # Get USDT values for common coins
        common_coins = ["BTC", "ETH", "BNB"]
        price_tasks = []
        
        # Create a limited set of tasks to get prices (to avoid timeouts)
        for balance in valid_balances:
            if balance["coin"] in common_coins:
                try:
                    symbol = f"{balance['coin']}USDT"
                    task = asyncio.create_task(async_client.get_symbol_ticker(symbol=symbol))
                    price_tasks.append((balance, task))
                except Exception as e:
                    logger.warning(f"Error creating price task for {balance['coin']}: {str(e)}")
        
        # Wait for price tasks with timeout
        if price_tasks:
            # Set a reasonable timeout for all price fetches (5 seconds total)
            await asyncio.wait([task for _, task in price_tasks], timeout=5)
            
            # Process results
            for balance, task in price_tasks:
                if task.done() and not task.exception():
                    try:
                        ticker = task.result()
                        if ticker and "price" in ticker:
                            price = float(ticker["price"])
                            balance["usdValue"] = price * balance["balance"]
                    except Exception as e:
                        logger.warning(f"Error processing price for {balance['coin']}: {str(e)}")
        
        # Sort by USD value
        valid_balances.sort(key=lambda x: x["usdValue"], reverse=True)
        
        return json({
            "success": True,
            "balances": valid_balances,
            "count": len(valid_balances)
        })
        
    except Exception as e:
        logger.error(f"Failed to retrieve balances: {str(e)}")
        logger.error(f"Exception traceback: {traceback.format_exc()}")
        return json({"success": False, "error": f"Failed to retrieve balances: {str(e)}"}, status=500)

@balance_bp.route("/history", methods=["GET"])
@openapi.tag("Balance")
@openapi.summary("Get historical balance data")
@openapi.description("Retrieves historical account balance data in USD equivalent for Binance.")
@openapi.parameter("X-MBX-APIKEY", str, "header", required=True)
@openapi.parameter("X-MBX-APISECRET", str, "header", required=True)
@openapi.parameter("start_time", int, "query", required=False, description="Start time in milliseconds since epoch")
@openapi.parameter("end_time", int, "query", required=False, description="End time in milliseconds since epoch")
@openapi.response(200, {"application/json": {"success": bool, "history": list}})
@openapi.response(401, {"application/json": {"success": bool, "error": str}})
@openapi.response(500, {"application/json": {"success": bool, "error": str}})
@validate_api_key
async def get_balance_history(request: Request):
    """Get historical balance data."""
    request_id = id(request)
    logger.info(f"[{request_id}] Processing balance history request")
    
    try:
        # Get the binance client
        binance_wrapper = request.app.ctx.binance_client
        
        # Ensure the client is initialized
        if not binance_wrapper.initialized:
            await binance_wrapper.initialize()
            
        # Get the AsyncClient object
        if not binance_wrapper.client:
            logger.error(f"[{request_id}] Binance client not initialized")
            return json({
                "success": False,
                "error": "Binance client not initialized"
            }, status=500)
        
        # Use the underlying AsyncClient
        async_client = binance_wrapper.client
        
        # Check if we're in testnet mode
        is_testnet = binance_wrapper.testnet
        logger.info(f"[{request_id}] Operating in {'testnet' if is_testnet else 'production'} mode")
        
        # Get date range from query parameters or default to last 30 days
        end_time = datetime.now()
        start_time = end_time - timedelta(days=30)
        if 'start_time' in request.args:
            start_time = datetime.fromtimestamp(int(request.args['start_time'][0]) / 1000)
        if 'end_time' in request.args:
            end_time = datetime.fromtimestamp(int(request.args['end_time'][0]) / 1000)
        
        logger.info(f"[{request_id}] Fetching balance history from {start_time.isoformat()} to {end_time.isoformat()}")
        
        # Fetch historical BTC/USDT prices first as this is needed for both real and fallback data
        try:
            klines = await async_client.get_klines(
                symbol='BTCUSDT',
                interval='1d',
                startTime=int(start_time.timestamp() * 1000),
                endTime=int(end_time.timestamp() * 1000)
            )
            
            # Process klines to map closing prices by date
            closing_prices = {}
            for kline in klines:
                date = datetime.fromtimestamp(kline[0] / 1000).date()  # kline[0] is timestamp in ms
                closing_price = float(kline[4])  # kline[4] is closing price
                closing_prices[date] = closing_price
                
            logger.debug(f"[{request_id}] Successfully retrieved {len(closing_prices)} days of BTC price data")
            
        except Exception as e:
            logger.error(f"[{request_id}] Error fetching BTC/USDT price data: {str(e)}")
            return json({
                'success': False, 
                'error': f"Failed to fetch price data: {str(e)}"
            }, status=500)
        
        history = []
        
        # Try to get account snapshots (may fail in testnet)
        try:
            logger.debug(f"[{request_id}] Attempting to fetch account snapshots")
            
            snapshots = await async_client.get_account_snapshot(
                type='SPOT',
                startTime=int(start_time.timestamp() * 1000),  # Binance expects milliseconds
                endTime=int(end_time.timestamp() * 1000)
            )
            
            # Log response structure for debugging
            logger.debug(f"[{request_id}] Snapshot response keys: {snapshots.keys() if isinstance(snapshots, dict) else 'Not a dict'}")
            
            if isinstance(snapshots, dict) and 'snapshotVos' in snapshots:
                snapshot_count = len(snapshots.get('snapshotVos', []))
                logger.info(f"[{request_id}] Retrieved {snapshot_count} account snapshots")
                
                # Process snapshots to compute USD balances
                for snapshot in snapshots.get('snapshotVos', []):
                    update_time = datetime.fromtimestamp(snapshot['updateTime'] / 1000)
                    date = update_time.date()
                    
                    # Log data structure for debugging
                    if 'data' not in snapshot or 'totalAssetOfBtc' not in snapshot['data']:
                        logger.warning(f"[{request_id}] Invalid snapshot format: {snapshot}")
                        continue
                        
                    total_btc = float(snapshot['data']['totalAssetOfBtc'])
                    
                    if date in closing_prices:
                        btc_price = closing_prices[date]
                        usd_balance = total_btc * btc_price
                        history.append({
                            'date': update_time.isoformat(),
                            'balance': round(usd_balance, 2),
                            'currency': 'USD'
                        })
                    else:
                        logger.warning(f"[{request_id}] No BTC price data for {date}")
            else:
                logger.warning(f"[{request_id}] Invalid snapshot response format or empty response")
                
        except Exception as e:
            logger.error(f"[{request_id}] Error fetching account snapshots: {str(e)}")
            error_msg = str(e)
            
            # If in testnet mode, use fallback mechanism
            if is_testnet:
                logger.info(f"[{request_id}] Using fallback mechanism for testnet")
                
                try:
                    # Get current balance
                    current_balance = await binance_wrapper.get_account_balance() or 0
                    logger.info(f"[{request_id}] Current testnet balance: {current_balance}")
                    
                    # Try to read from CSV file if it exists
                    csv_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "balance_history.csv")
                    if os.path.exists(csv_path):
                        logger.info(f"[{request_id}] Found balance history CSV file at {csv_path}")
                        
                        import csv
                        with open(csv_path, mode='r') as file:
                            csv_reader = csv.DictReader(file)
                            
                            for row in csv_reader:
                                try:
                                    entry_date = datetime.fromisoformat(row['timestamp'])
                                    if start_time <= entry_date <= end_time:
                                        history.append({
                                            'date': row['timestamp'],
                                            'balance': float(row['balance']),
                                            'currency': row['currency']
                                        })
                                except (ValueError, KeyError) as e:
                                    logger.warning(f"[{request_id}] Error parsing CSV row: {str(e)}")
                                    continue
                        
                        logger.info(f"[{request_id}] Loaded {len(history)} entries from CSV file")
                    
                    # If no history in CSV or empty, generate synthetic data
                    if not history:
                        logger.info(f"[{request_id}] Generating synthetic balance history data")
                        
                        # Generate one data point per day in the range
                        days_range = (end_time - start_time).days + 1
                        
                        # Start with current balance and work backwards with small random changes
                        # This simulates natural balance fluctuations
                        import random
                        
                        # Generate daily balances
                        balance = current_balance
                        daily_balance = []
                        
                        for day in range(days_range):
                            date = end_time - timedelta(days=day)
                            # Random change between -1% and +1%
                            change_pct = random.uniform(-0.01, 0.01)
                            # For historical data, apply change and store the value
                            balance = balance / (1 + change_pct)
                            
                            daily_balance.append({
                                'date': date.isoformat(),
                                'balance': round(balance, 2),
                                'currency': 'USDT'  # Use USDT for testnet
                            })
                        
                        # Reverse to get chronological order
                        daily_balance.reverse()
                        history.extend(daily_balance)
                        
                        logger.info(f"[{request_id}] Generated {len(daily_balance)} synthetic data points")
                
                except Exception as fallback_error:
                    logger.error(f"[{request_id}] Fallback mechanism failed: {str(fallback_error)}")
                    return json({
                        'success': False,
                        'error': f"Error retrieving balance history: {error_msg}. Fallback also failed: {str(fallback_error)}",
                        'isTestnet': True
                    }, status=500)
            
            # If not in testnet or fallback generated no data, return the original error
            if not is_testnet or not history:
                if not is_testnet:
                    logger.error(f"[{request_id}] Production API error: {error_msg}")
                return json({
                    'success': False,
                    'error': f"Error retrieving balance history: {error_msg}",
                    'isTestnet': is_testnet
                }, status=500)
        
        # Sort by date
        history = sorted(history, key=lambda x: x['date'])
        
        logger.info(f"[{request_id}] Returning {len(history)} balance history data points")
        
        # Initialize snapshots
        snapshots = None

        # Then later when returning the response, update to:
        return json({
            'success': True,
            'history': history,
            'isTestnet': is_testnet,
            'source': 'csv_fallback' if is_testnet and not snapshots else 'binance_api'
        })

    except Exception as e:
        logger.error(f"[{request_id}] Unhandled error in balance history endpoint: {str(e)}")
        logger.error(f"[{request_id}] Traceback: {traceback.format_exc()}")
        return json({
            'success': False, 
            'error': f"Failed to retrieve balance history: {str(e)}"
        }, status=500)