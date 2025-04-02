# Create trading_execute.py
import asyncio
import pandas as pd
import numpy as np
import logging
from app.utils.binance_utils import BinanceClient
from app.utils.risk_manager import RiskManager
from app.config import config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def execute_trade(symbol="SOLUSDT", interval="1h", execute=False):
    """Execute a trade based on adaptive strategy."""
    try:
        # Initialize clients
        api_key, api_secret = config.get_binance_credentials()
        binance_client = BinanceClient(api_key, api_secret, testnet=config.ENV_MODE == "testnet")
        await binance_client.initialize()
        
        # Initialize risk manager
        risk_manager = RiskManager(
            max_risk_percent=1.0,
            risk_reward_ratio=2.0,
            max_positions=3
        )
        
        # Get historical data - request maximum
        klines = await binance_client.get_klines(symbol, interval, 1000)
        if not klines:
            raise ValueError("Failed to fetch klines data")
        
        # Convert to DataFrame
        df = pd.DataFrame(klines, columns=[
            'timestamp', 'open', 'high', 'low', 'close', 'volume',
            'close_time', 'quote_asset_volume', 'number_of_trades',
            'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume', 'ignore'
        ])
        
        # Convert numeric columns
        for col in ['open', 'high', 'low', 'close', 'volume']:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        
        logger.info(f"Got {len(df)} candles for {symbol} at {interval} timeframe")
        
        # Calculate indicators
        import pandas_ta as ta
        
        available_periods = min(len(df) - 10, 50)
        df['sma20'] = ta.sma(df['close'], length=min(20, available_periods))
        df['sma50'] = ta.sma(df['close'], length=min(50, available_periods))
        df['rsi'] = ta.rsi(df['close'], length=min(14, available_periods))
        df['atr'] = ta.atr(df['high'], df['low'], df['close'], length=min(14, available_periods))
        
        # Strong uptrend signal - more conservative
        buy_condition = (
            (df['close'] > df['sma20']) &  # Price above SMA20
            (df['sma20'] > df['sma50']) &  # SMA20 above SMA50 (uptrend)
            (df['rsi'] > 45) &             # RSI showing momentum
            (df['rsi'] < 65) &             # But not overbought
            (df['volume'] > df['volume'].rolling(10).mean() * 1.2)  # Volume confirmation
        )
        
        # Risk management signal
        sell_condition = (
            (df['close'] < df['sma20']) |  # Price below SMA20
            (df['rsi'] > 75)               # RSI overbought
        )
        
        # Check if we have a signal on the latest candle
        latest = -1
        buy_signal = buy_condition.iloc[latest]
        sell_signal = sell_condition.iloc[latest]
        
        # Get current price and calculate risk
        current_price = float(df['close'].iloc[latest])
        current_atr = float(df['atr'].iloc[latest])
        
        # Calculate suggested stop loss and take profit
        stop_loss = current_price - (current_atr * 1.5)  # Tighter stop for altcoins
        take_profit = current_price + (current_atr * 3)
        
        # Get account balance and calculate position size
        balance = await binance_client.get_account_balance() or 0.0
        logger.info(f"Account balance: ${balance:.2f} USDT")
        
        # Analysis output
        logger.info(f"Analysis of {symbol} at {interval}:")
        logger.info(f"Current price: ${current_price:.2f}")
        logger.info(f"ATR: ${current_atr:.2f}")
        logger.info(f"Signal: {'BUY' if buy_signal else 'SELL' if sell_signal else 'NONE'}")
        
        # Execute trade if signal and execution is enabled
        if buy_signal:
            # Calculate position size (limit to $10 for initial testing)
            max_position_value = 10.0
            position_size = risk_manager.calculate_position_size(
                account_balance=balance,
                entry_price=current_price,
                stop_loss=stop_loss,
                symbol=symbol
            )
            
            # Limit position value to $10 for initial testing
            if position_size * current_price > max_position_value:
                position_size = max_position_value / current_price
            
            logger.info(f"Suggested entry: ${current_price:.2f}")
            logger.info(f"Suggested stop loss: ${stop_loss:.2f}")
            logger.info(f"Suggested take profit: ${take_profit:.2f}")
            logger.info(f"Suggested position size: {position_size:.6f} units (${position_size * current_price:.2f})")
            
            if execute:
                logger.info("Executing BUY order...")
                order_id = await binance_client.create_buy_order(
                    symbol=symbol,
                    quantity=position_size,
                    price=current_price,
                    stop_loss=stop_loss,
                    take_profit=take_profit
                )
                if order_id:
                    logger.info(f"Order placed successfully! Order ID: {order_id}")
                else:
                    logger.error("Failed to place order")
            else:
                logger.info("Execute mode is OFF. This is a simulation only.")
        
        elif sell_signal and execute:
            # Check if we have an open position
            logger.info("Checking for open positions to exit...")
            success = await binance_client.exit_position(symbol)
            if success:
                logger.info(f"Successfully exited position for {symbol}")
            else:
                logger.info(f"No open position found for {symbol}")
        
        # Close connection
        await binance_client.close()
        
    except Exception as e:
        logger.error(f"Error executing trade: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())

if __name__ == "__main__":
    import sys
    import argparse
    
    parser = argparse.ArgumentParser(description='Execute cryptocurrency trades')
    parser.add_argument('symbol', type=str, help='Trading pair symbol (e.g., SOLUSDT)')
    parser.add_argument('--interval', type=str, default="1h", help='Timeframe (1h, 4h, 1d)')
    parser.add_argument('--execute', action='store_true', help='Execute actual trades (default: simulation only)')
    
    args = parser.parse_args()
    
    asyncio.run(execute_trade(args.symbol, args.interval, args.execute))