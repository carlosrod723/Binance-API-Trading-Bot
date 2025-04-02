# app/api/positions.py

from sanic import Blueprint
from sanic.request import Request
from sanic.response import json
from sanic_ext import openapi
from functools import wraps
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type
import asyncio
import logging
import json as json_lib
import time
import traceback
from typing import Dict, Any, List, Optional

logger = logging.getLogger(__name__)

positions_bp = Blueprint("positions", url_prefix="/api/v1/positions")

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

@positions_bp.route("/open", methods=["GET"])
@openapi.tag("Positions")
@openapi.summary("Get open positions")
@openapi.description("Retrieves a list of currently open positions from Binance.")
@openapi.parameter("X-MBX-APIKEY", str, "header", required=True, description="Binance API Key")
@openapi.parameter("X-MBX-APISECRET", str, "header", required=True, description="Binance API Secret")
@openapi.parameter("symbol", str, "query", required=False, description="Filter by specific trading pair")
@openapi.response(200, {"application/json": {"success": bool, "positions": List[Dict[str, Any]], "count": int, "timestamp": int}})
@openapi.response(400, {"application/json": {"success": bool, "error": str, "timestamp": int}})
@openapi.response(401, {"application/json": {"success": bool, "error": str, "timestamp": int}})
@openapi.response(500, {"application/json": {"success": bool, "error": str, "timestamp": int}})
@openapi.response(503, {"application/json": {"success": bool, "error": str, "timestamp": int}})
@validate_api_key
@retry(
    stop=stop_after_attempt(3), 
    wait=wait_fixed(2),
    retry=retry_if_exception_type((ConnectionError, TimeoutError))
)
async def get_open_positions(request: Request):
    """Handle requests for open positions on Binance with filtering options."""
    request_id = id(request)
    logger.info(f"[{request_id}] Processing open positions request")
    
    # Get optional symbol filter
    symbol = request.args.get("symbol")
    if symbol:
        logger.info(f"[{request_id}] Filtering open positions by symbol: {symbol}")
    
    client = request.app.ctx.binance_client
    if not client:
        logger.error(f"[{request_id}] Binance client not initialized")
        return json(
            standard_response(False, error="Trading service unavailable"), 
            status=503
        )
    
    try:
        positions = await client.get_open_positions()
        
        # Apply symbol filter if provided
        if symbol and positions:
            positions = [p for p in positions if p.get('coinPair') == symbol]
            
        logger.info(f"[{request_id}] Retrieved {len(positions)} open positions")
        
        return json(
            standard_response(True, {
                "positions": positions,
                "count": len(positions),
                "lastUpdated": int(time.time() * 1000)
            })
        )
        
    except ConnectionError as ce:
        logger.error(f"[{request_id}] Connection error fetching open positions: {str(ce)}")
        return json(
            standard_response(False, error="Connection error with exchange service"), 
            status=503
        )
    except Exception as e:
        logger.error(f"[{request_id}] Failed to fetch open positions: {str(e)}")
        logger.debug(f"[{request_id}] Exception traceback: {traceback.format_exc()}")
        return json(
            standard_response(False, error=f"Failed to fetch open positions: {str(e)}"), 
            status=500
        )

@positions_bp.route("/closed", methods=["GET"])
@openapi.tag("Positions")
@openapi.summary("Get closed positions")
@openapi.description("Retrieves a list of recently closed positions from Binance with filtering options.")
@openapi.parameter("X-MBX-APIKEY", str, "header", required=True, description="Binance API Key")
@openapi.parameter("X-MBX-APISECRET", str, "header", required=True, description="Binance API Secret")
@openapi.parameter("symbol", str, "query", required=True, description="Filter by specific trading pair")
@openapi.parameter("limit", int, "query", required=False, description="Max positions (default: 100, max: 500)")
@openapi.parameter("startTime", int, "query", required=False, description="Start time (Unix ms)")
@openapi.parameter("endTime", int, "query", required=False, description="End time (Unix ms)")
@openapi.parameter("profitType", str, "query", required=False, description="Filter by profit type (profitable, unprofitable, all)")
@openapi.response(200, {"application/json": {"success": bool, "positions": List[Dict[str, Any]], "count": int, "timestamp": int}})
@openapi.response(400, {"application/json": {"success": bool, "error": str, "timestamp": int}})
@openapi.response(401, {"application/json": {"success": bool, "error": str, "timestamp": int}})
@openapi.response(500, {"application/json": {"success": bool, "error": str, "timestamp": int}})
@openapi.response(503, {"application/json": {"success": bool, "error": str, "timestamp": int}})
@validate_api_key
@retry(
    stop=stop_after_attempt(3), 
    wait=wait_fixed(2),
    retry=retry_if_exception_type((ConnectionError, TimeoutError))
)
async def get_closed_positions(request: Request):
    """Handle requests for closed positions on Binance with advanced filtering."""
    request_id = id(request)
    logger.info(f"[{request_id}] Processing closed positions request")
    
    # Collect and validate query parameters
    errors = []
    params = {}
    
    # Get required symbol parameter
    symbol = request.args.get("symbol")
    if not symbol:
        # For testing purposes, provide a default symbol
        if hasattr(request.app.ctx, "config") and request.app.ctx.config.ENV_MODE == "testnet":
            symbol = "BTCUSDT"
            logger.info(f"[{request_id}] Using default symbol {symbol} for testnet")
        else:
            logger.warning(f"[{request_id}] Missing required symbol parameter")
            return json(
                standard_response(False, error="Symbol parameter is required"), 
                status=400
            )
    
    # Validate limit
    limit = 100  # Default
    limit_str = request.args.get("limit")
    if limit_str:
        try:
            limit_val = int(limit_str)
            if limit_val <= 0:
                errors.append("Limit must be a positive integer")
            else:
                # Cap at 500 to prevent performance issues
                limit = min(limit_val, 500)
                params["limit"] = limit
        except ValueError:
            errors.append("Limit must be a valid integer")
    else:
        params["limit"] = limit
    
    # Validate start/end times
    for param_name, param_key in [("startTime", "start_time"), ("endTime", "end_time")]:
        param_str = request.args.get(param_name)
        if param_str:
            try:
                param_val = int(param_str)
                if param_val <= 0:
                    errors.append(f"{param_name} must be a positive integer")
                else:
                    params[param_key] = param_val
            except ValueError:
                errors.append(f"{param_name} must be a valid integer")
        else:
            params[param_key] = None
    
    # Get optional profit type filter
    profit_type = request.args.get("profitType")
    if profit_type and profit_type not in ["profitable", "unprofitable", "all"]:
        errors.append("profitType must be one of: profitable, unprofitable, all")
    
    # Return validation errors
    if errors:
        error_message = "; ".join(errors)
        logger.warning(f"[{request_id}] Invalid parameters: {error_message}")
        return json(
            standard_response(False, error=error_message), 
            status=400
        )
    
    client = request.app.ctx.binance_client
    if not client:
        logger.error(f"[{request_id}] Binance client not initialized")
        return json(
            standard_response(False, error="Trading service unavailable"), 
            status=503
        )
    
    try:
        # Get positions with specified parameters
        positions = await client.get_closed_positions(
            symbol=symbol,
            limit=params["limit"],
            start_time=params["start_time"],
            end_time=params["end_time"]
        )
        
        # Apply profit type filter if specified
        if profit_type and profit_type != "all":
            if profit_type == "profitable":
                positions = [p for p in positions if p.get('profit', 0) > 0]
            elif profit_type == "unprofitable":
                positions = [p for p in positions if p.get('profit', 0) <= 0]
        
        logger.info(f"[{request_id}] Retrieved {len(positions)} closed positions")
        
        # Calculate stats for response
        total_profit = sum(p.get('profit', 0) for p in positions)
        avg_profit_percent = sum(p.get('profitPercent', 0) for p in positions) / len(positions) if positions else 0
        
        return json(
            standard_response(True, {
                "positions": positions,
                "count": len(positions),
                "stats": {
                    "totalProfit": total_profit,
                    "averageProfitPercent": avg_profit_percent,
                    "profitableCount": sum(1 for p in positions if p.get('profit', 0) > 0),
                    "unprofitableCount": sum(1 for p in positions if p.get('profit', 0) <= 0)
                }
            })
        )
        
    except ConnectionError as ce:
        logger.error(f"[{request_id}] Connection error fetching closed positions: {str(ce)}")
        return json(
            standard_response(False, error="Connection error with exchange service"), 
            status=503
        )
    except Exception as e:
        logger.error(f"[{request_id}] Failed to fetch closed positions: {str(e)}")
        logger.debug(f"[{request_id}] Exception traceback: {traceback.format_exc()}")
        return json(
            standard_response(False, error=f"Failed to fetch closed positions: {str(e)}"), 
            status=500
        )

@positions_bp.websocket("/open/ws")
@openapi.tag("Positions")
@openapi.summary("WebSocket for open positions")
@openapi.description("Streams real-time updates of currently open positions from Binance.")
@openapi.parameter("X-MBX-APIKEY", str, "header", required=True, description="Binance API Key")
@openapi.parameter("X-MBX-APISECRET", str, "header", required=True, description="Binance API Secret")
@openapi.parameter("interval", int, "query", required=False, description="Update interval in seconds (min: 5, max: 60, default: 10)")
@openapi.parameter("symbol", str, "query", required=False, description="Filter by specific trading pair")
async def open_positions_websocket(request: Request, ws):
    """WebSocket handler for real-time open positions updates with enhanced features."""
    request_id = id(request)
    logger.info(f"[{request_id}] WebSocket connection opened for open positions")
    
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
    
    # Get client configuration
    symbol = request.args.get("symbol")
    interval_str = request.args.get("interval", "10")
    
    # Validate interval
    try:
        interval = int(interval_str)
        if interval < 5:
            interval = 5  # Minimum interval to prevent API rate limiting
        elif interval > 60:
            interval = 60  # Maximum interval for responsiveness
    except ValueError:
        logger.warning(f"[{request_id}] Invalid interval: {interval_str}")
        await ws.send(json_lib.dumps(
            standard_response(False, error="Invalid interval, must be an integer")
        ))
        await ws.close(1008, "Invalid parameters")
        return
    
    # Track position details for comparison
    position_snapshots = {}
    client = request.app.ctx.binance_client
    
    if not client:
        logger.error(f"[{request_id}] Binance client not initialized")
        await ws.send(json_lib.dumps(
            standard_response(False, error="Trading service unavailable")
        ))
        await ws.close(1011, "Service unavailable")
        return
    
    # Heartbeat mechanism
    last_update_time = time.time()
    heartbeat_interval = min(interval, 30)  # Ensure at least 30s heartbeat
    
    # Start a separate task for heartbeats
    async def send_heartbeats():
        while not ws.closed:
            current_time = time.time()
            if current_time - last_update_time >= heartbeat_interval:
                try:
                    await ws.send(json_lib.dumps({
                        "type": "heartbeat",
                        "timestamp": int(current_time * 1000)
                    }))
                    logger.debug(f"[{request_id}] Sent heartbeat")
                except Exception as e:
                    logger.error(f"[{request_id}] Failed to send heartbeat: {str(e)}")
                    break
            await asyncio.sleep(heartbeat_interval)
    
    # Start the heartbeat task
    heartbeat_task = asyncio.create_task(send_heartbeats())
    
    # Send initial connection confirmation
    try:
        await ws.send(json_lib.dumps(
            standard_response(True, {
                "message": "Connected to open positions stream",
                "configuration": {
                    "interval": interval,
                    "symbol": symbol if symbol else "all"
                }
            })
        ))

    except Exception as e:
        logger.error(f"[{request_id}] Failed to send initial message: {str(e)}")
        if not heartbeat_task.done():
            heartbeat_task.cancel()
        return
    
    # Initialization step - get current positions to establish baseline
    try:
        positions = await client.get_open_positions()
        
        # Apply symbol filter if provided
        if symbol and positions:
            positions = [p for p in positions if p.get('coinPair') == symbol]
            
        # Record initial position details
        for position in positions:
            pos_id = position.get('id')
            if pos_id:
                position_snapshots[pos_id] = position
        
        # Send initial positions
        await ws.send(json_lib.dumps(
            standard_response(True, {
                "positions": positions,
                "count": len(positions),
                "initial": True,
                "lastUpdated": int(time.time() * 1000)
            })
        ))
        
        last_update_time = time.time()
        
    except Exception as e:
        logger.error(f"[{request_id}] Error initializing open positions: {str(e)}")
        try:
            await ws.send(json_lib.dumps(
                standard_response(False, error=f"Failed to initialize: {str(e)}")
            ))
        except Exception:
            pass
        
        if not heartbeat_task.done():
            heartbeat_task.cancel()
        await ws.close(1011, "Initialization failed")
        return
    
    # Main update loop
    try:
        retry_count = 0
        max_retries = 5
        
        while not ws.closed:
            try:
                # Get current open positions
                positions = await client.get_open_positions()
                
                # Apply symbol filter if provided
                if symbol and positions:
                    positions = [p for p in positions if p.get('coinPair') == symbol]
                
                # Identify current position IDs
                current_position_ids = {p.get('id') for p in positions if p.get('id')}
                
                # Check for new, updated, and closed positions
                new_positions = []
                updated_positions = []
                closed_position_ids = set(position_snapshots.keys()) - current_position_ids
                
                for position in positions:
                    pos_id = position.get('id')
                    if not pos_id:
                        continue
                        
                    if pos_id not in position_snapshots:
                        # New position
                        new_positions.append(position)
                        position_snapshots[pos_id] = position
                    else:
                        # Check if position details have changed significantly
                        old_position = position_snapshots[pos_id]
                        
                        # Compare key fields that indicate meaningful changes
                        significant_change = False
                        
                        # Check price changes (more than 0.5%)
                        old_price = old_position.get('currentPrice', 0)
                        new_price = position.get('currentPrice', 0)
                        if old_price > 0 and abs((new_price - old_price) / old_price) > 0.005:
                            significant_change = True
                            
                        # Check profit changes (more than $1 or 1%)
                        old_profit = old_position.get('profit', 0)
                        new_profit = position.get('profit', 0)
                        if abs(new_profit - old_profit) > 1 or (old_profit != 0 and abs((new_profit - old_profit) / old_profit) > 0.01):
                            significant_change = True
                            
                        # Check if stop loss or take profit changed
                        if position.get('stopLoss') != old_position.get('stopLoss') or position.get('takeProfit') != old_position.get('takeProfit'):
                            significant_change = True
                            
                        # Check if status changed
                        if position.get('status') != old_position.get('status'):
                            significant_change = True
                            
                        if significant_change:
                            updated_positions.append(position)
                            # Update snapshot
                            position_snapshots[pos_id] = position
                
                # Prepare message data if there are changes
                has_changes = new_positions or updated_positions or closed_position_ids
                
                if has_changes:
                    # Get closed position details
                    closed_positions = [position_snapshots[pos_id] for pos_id in closed_position_ids]
                    
                    # Remove closed positions from tracking
                    for pos_id in closed_position_ids:
                        if pos_id in position_snapshots:
                            del position_snapshots[pos_id]
                    
                    # Prepare updates message
                    update_data = {
                        "new": new_positions,
                        "updated": updated_positions,
                        "closed": closed_positions,
                        "changes": {
                            "new": len(new_positions),
                            "updated": len(updated_positions),
                            "closed": len(closed_position_ids)
                        },
                        "lastUpdated": int(time.time() * 1000)
                    }
                    
                    logger.info(f"[{request_id}] Position changes detected: {len(new_positions)} new, {len(updated_positions)} updated, {len(closed_position_ids)} closed")
                    
                    await ws.send(json_lib.dumps(
                        standard_response(True, update_data)
                    ))
                    
                    last_update_time = time.time()
                    retry_count = 0  # Reset retry count on successful update
                
                # Send a full snapshot periodically (every 10 intervals)
                if time.time() - last_update_time > interval * 10:
                    await ws.send(json_lib.dumps(
                        standard_response(True, {
                            "positions": positions,
                            "count": len(positions),
                            "snapshot": True,
                            "lastUpdated": int(time.time() * 1000)
                        })
                    ))
                    last_update_time = time.time()
                
                await asyncio.sleep(interval)
                
            except asyncio.CancelledError:
                logger.info(f"[{request_id}] WebSocket task cancelled")
                break
                
            except Exception as e:
                retry_count += 1
                logger.error(f"[{request_id}] Error updating open positions: {str(e)}")
                
                if retry_count <= max_retries:
                    # Exponential backoff for retries
                    retry_delay = min(2 ** retry_count, 30)
                    logger.info(f"[{request_id}] Retrying in {retry_delay}s ({retry_count}/{max_retries})")
                    
                    try:
                        await ws.send(json_lib.dumps(
                            standard_response(False, {
                                "message": f"Error fetching positions, retrying in {retry_delay}s",
                                "retryCount": retry_count,
                                "maxRetries": max_retries
                            }, error=str(e))
                        ))
                    except Exception:
                        logger.error(f"[{request_id}] Failed to send error message to client")
                        break
                        
                    await asyncio.sleep(retry_delay)
                else:
                    logger.error(f"[{request_id}] Max retries ({max_retries}) reached, closing connection")
                    try:
                        await ws.send(json_lib.dumps(
                            standard_response(False, error=f"Failed to fetch positions after {max_retries} attempts")
                        ))
                    except Exception:
                        pass
                    break
    
    except Exception as e:
        logger.error(f"[{request_id}] Unhandled exception in WebSocket: {str(e)}")
        logger.debug(f"[{request_id}] Exception traceback: {traceback.format_exc()}")
    
    finally:
        # Clean up resources
        if not heartbeat_task.done():
            heartbeat_task.cancel()
            try:
                await heartbeat_task
            except asyncio.CancelledError:
                pass
                
        logger.info(f"[{request_id}] WebSocket connection for open positions closed")
        
        # Ensure WebSocket is closed
        if not ws.closed:
            try:
                await ws.close(1000, "Connection terminated")
            except Exception:
                pass

@positions_bp.websocket("/closed/ws")
@openapi.tag("Positions")
@openapi.summary("WebSocket for closed positions")
@openapi.description("Streams real-time updates of newly closed positions from Binance.")
@openapi.parameter("X-MBX-APIKEY", str, "header", required=True, description="Binance API Key")
@openapi.parameter("X-MBX-APISECRET", str, "header", required=True, description="Binance API Secret")
@openapi.parameter("interval", int, "query", required=False, description="Update interval in seconds (min: 10, max: 300, default: 60)")
@openapi.parameter("symbol", str, "query", required=False, description="Filter by specific trading pair")
@openapi.parameter("limit", int, "query", required=False, description="Number of positions to check (default: 20, max: 100)")
async def closed_positions_websocket(request: Request, ws):
    """WebSocket handler for real-time closed positions updates with enhanced features."""
    request_id = id(request)
    logger.info(f"[{request_id}] WebSocket connection opened for closed positions")
    
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
    
    # Get client configuration
    symbol = request.args.get("symbol")
    interval_str = request.args.get("interval", "60")
    limit_str = request.args.get("limit", "20")
    
    # Validate interval
    try:
        interval = int(interval_str)
        if interval < 10:
            interval = 10  # Minimum interval to prevent load
        elif interval > 300:
            interval = 300  # Maximum interval
    except ValueError:
        logger.warning(f"[{request_id}] Invalid interval: {interval_str}")
        await ws.send(json_lib.dumps(
            standard_response(False, error="Invalid interval, must be an integer")
        ))
        await ws.close(1008, "Invalid parameters")
        return
    
    # Validate limit
    try:
        limit = int(limit_str)
        if limit < 1:
            limit = 1
        elif limit > 100:
            limit = 100  # Cap at 100 to prevent excessive data transfer
    except ValueError:
        logger.warning(f"[{request_id}] Invalid limit: {limit_str}")
        await ws.send(json_lib.dumps(
            standard_response(False, error="Invalid limit, must be an integer")
        ))
        await ws.close(1008, "Invalid parameters")
        return
    
    # Last known positions IDs to detect new positions
    last_position_ids = set()
    client = request.app.ctx.binance_client
    
    if not client:
        logger.error(f"[{request_id}] Binance client not initialized")
        await ws.send(json_lib.dumps(
            standard_response(False, error="Trading service unavailable")
        ))
        await ws.close(1011, "Service unavailable")
        return
    
    # Heartbeat mechanism
    last_update_time = time.time()
    heartbeat_interval = min(interval, 30)  # Ensure at least 30s heartbeat
    
    # Start a separate task for heartbeats
    async def send_heartbeats():
        while not ws.closed:
            current_time = time.time()
            if current_time - last_update_time >= heartbeat_interval:
                try:
                    await ws.send(json_lib.dumps({
                        "type": "heartbeat",
                        "timestamp": int(current_time * 1000)
                    }))
                    logger.debug(f"[{request_id}] Sent heartbeat")
                except Exception as e:
                    logger.error(f"[{request_id}] Failed to send heartbeat: {str(e)}")
                    break
            await asyncio.sleep(heartbeat_interval)
    
    # Start the heartbeat task
    heartbeat_task = asyncio.create_task(send_heartbeats())
    
    # Send initial connection confirmation
    try:
        await ws.send(json_lib.dumps(
            standard_response(True, {
                "message": "Connected to closed positions stream",
                "configuration": {
                    "interval": interval,
                    "symbol": symbol,
                    "limit": limit
                }
            })
        ))

    except Exception as e:
        logger.error(f"[{request_id}] Failed to send initial message: {str(e)}")
        if not heartbeat_task.done():
            heartbeat_task.cancel()
        return
    
    # Initialization step - get current positions to establish baseline
    try:
        positions = await client.get_closed_positions(
            symbol=symbol,
            limit=limit
        )
        
        # Record IDs of initial positions
        last_position_ids = {p.get('id') for p in positions if p.get('id')}
        
        # Send initial positions
        await ws.send(json_lib.dumps(
            standard_response(True, {
                "positions": positions,
                "count": len(positions),
                "initial": True,
                "lastUpdated": int(time.time() * 1000)
            })
        ))
        
        last_update_time = time.time()
        
    except Exception as e:
        logger.error(f"[{request_id}] Error initializing closed positions: {str(e)}")
        try:
            await ws.send(json_lib.dumps(
                standard_response(False, error=f"Failed to initialize: {str(e)}")
            ))
        except Exception:
            pass
        
        if not heartbeat_task.done():
            heartbeat_task.cancel()
        await ws.close(1011, "Initialization failed")
        return
    
    # Main update loop
    try:
        retry_count = 0
        max_retries = 5
        
        while not ws.closed:
            try:
                positions = await client.get_closed_positions(
                    symbol=symbol,
                    limit=limit
                )
                
                # Identify new positions
                current_position_ids = {p.get('id') for p in positions if p.get('id')}
                new_position_ids = current_position_ids - last_position_ids
                
                if new_position_ids:
                    # Get new positions that weren't in the last update
                    new_positions = [p for p in positions if p.get('id') in new_position_ids]
                    
                    logger.info(f"[{request_id}] Found {len(new_positions)} new closed positions")
                    
                    await ws.send(json_lib.dumps(
                        standard_response(True, {
                            "positions": new_positions,
                            "count": len(new_positions),
                            "new": True,
                            "lastUpdated": int(time.time() * 1000)
                        })
                    ))
                    
                    # Update last seen position IDs
                    last_position_ids = current_position_ids
                    last_update_time = time.time()
                    retry_count = 0  # Reset retry count on successful update
                
                await asyncio.sleep(interval)
                
            except asyncio.CancelledError:
                logger.info(f"[{request_id}] WebSocket task cancelled")
                break
                
            except Exception as e:
                retry_count += 1
                logger.error(f"[{request_id}] Error updating closed positions: {str(e)}")
                
                if retry_count <= max_retries:
                    # Exponential backoff for retries
                    retry_delay = min(2 ** retry_count, 60)
                    logger.info(f"[{request_id}] Retrying in {retry_delay}s ({retry_count}/{max_retries})")
                    
                    try:
                        await ws.send(json_lib.dumps(
                            standard_response(False, {
                                "message": f"Error fetching positions, retrying in {retry_delay}s",
                                "retryCount": retry_count,
                                "maxRetries": max_retries
                            }, error=str(e))
                        ))
                    except Exception:
                        logger.error(f"[{request_id}] Failed to send error message to client")
                        break
                        
                    await asyncio.sleep(retry_delay)
                else:
                    logger.error(f"[{request_id}] Max retries ({max_retries}) reached, closing connection")
                    try:
                        await ws.send(json_lib.dumps(
                            standard_response(False, error=f"Failed to fetch positions after {max_retries} attempts")
                        ))
                    except Exception:
                        pass
                    break
    
    except Exception as e:
        logger.error(f"[{request_id}] Unhandled exception in WebSocket: {str(e)}")
        logger.debug(f"[{request_id}] Exception traceback: {traceback.format_exc()}")
    
    finally:
        # Clean up resources
        if not heartbeat_task.done():
            heartbeat_task.cancel()
            try:
                await heartbeat_task
            except asyncio.CancelledError:
                pass
                
        logger.info(f"[{request_id}] WebSocket connection for closed positions closed")
        
        # Ensure WebSocket is closed
        if not ws.closed:
            try:
                await ws.close(1000, "Connection terminated")
            except Exception:
                pass

@positions_bp.route("/distribution", methods=["GET"])
@openapi.tag("Positions")
@openapi.summary("Get position distribution")
@openapi.description("Retrieves position distribution analytics by coin and strategy.")
@openapi.parameter("X-MBX-APIKEY", str, "header", required=True)
@openapi.parameter("X-MBX-APISECRET", str, "header", required=True)
@openapi.parameter("symbol", str, "query", required=False)
@openapi.response(200, {"application/json": {"success": bool, "data": dict}})
@openapi.response(401, {"application/json": {"success": bool, "error": str}})
@openapi.response(500, {"application/json": {"success": bool, "error": str}})
@validate_api_key
async def get_position_distribution(request: Request):
    """Handle requests for position distribution analytics."""
    request_id = id(request)
    logger.info(f"[{request_id}] Processing position distribution request")
    
    try:
        # Get open positions first
        client = request.app.ctx.binance_client
        if not client:
            logger.error(f"[{request_id}] Binance client not initialized")
            return json(
                standard_response(False, error="Trading service unavailable"), 
                status=503
            )
            
        positions = await client.get_open_positions()
        
        # Apply symbol filter if provided
        symbol = request.args.get("symbol")
        if symbol and positions:
            positions = [p for p in positions if p.get('coinPair') == symbol]
            
        # Calculate coin distribution
        coin_map = {}
        for position in positions:
            symbol = position.get('coinPair', 'Unknown')
            if symbol not in coin_map:
                coin_map[symbol] = 0
            coin_map[symbol] += 1
            
        # Convert to percentage
        total = len(positions)
        coin_distribution = [
            {"name": coin, "value": round((count / total * 100) if total > 0 else 0)} 
            for coin, count in coin_map.items()
        ]
        
        # Get strategies from position data if available
        strategy_map = {}
        for position in positions:
            strategy = position.get('strategy', 'Unknown')
            if strategy not in strategy_map:
                strategy_map[strategy] = 0
            strategy_map[strategy] += 1
            
        # Convert to percentage
        strategy_distribution = [
            {"name": strategy, "value": round((count / total * 100) if total > 0 else 0)} 
            for strategy, count in strategy_map.items()
        ]
        
        # Calculate profit by strategy
        profit_by_strategy = []
        strategy_profit_map = {}
        
        for position in positions:
            strategy = position.get('strategy', 'Unknown')
            profit = position.get('profit', 0)
            if strategy not in strategy_profit_map:
                strategy_profit_map[strategy] = 0
            strategy_profit_map[strategy] += profit
            
        profit_by_strategy = [
            {"name": strategy, "profit": profit} 
            for strategy, profit in strategy_profit_map.items()
        ]
        
        # Create response data
        data = {
            "coinDistribution": coin_distribution,
            "strategyDistribution": strategy_distribution,
            "profitByStrategy": profit_by_strategy
        }
        
        return json({
            "success": True,
            "data": data
        })
        
    except Exception as e:
        logger.error(f"[{request_id}] Failed to get position distribution: {str(e)}")
        return json({
            "success": False, 
            "error": f"Failed to get position distribution: {str(e)}"
        }, status=500)

# WebSocket endpoint if needed
@positions_bp.websocket("/distribution/ws")
@openapi.tag("Positions")
async def position_distribution_websocket(request: Request, ws):
    # Implementation
    pass