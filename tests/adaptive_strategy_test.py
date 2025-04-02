import asyncio
import pandas as pd
import numpy as np
import logging
from app.utils.binance_utils import BinanceClient
from app.utils.risk_manager import RiskManager
from app.config import config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def adaptive_strategy_test(symbol="BTCUSDT", interval="1h"):
    """Test a simple adaptive strategy with available data."""
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
        
        # Calculate indicators adaptively based on available data
        import pandas_ta as ta
        
        # Use shorter lookback periods for available data
        available_periods = min(len(df) - 10, 50)  # Cap at 50 or available data
        logger.info(f"Using adaptive lookback of {available_periods} periods")
        
        # Calculate simple indicators
        df['sma20'] = ta.sma(df['close'], length=min(20, available_periods))
        df['sma50'] = ta.sma(df['close'], length=min(50, available_periods))
        df['rsi'] = ta.rsi(df['close'], length=min(14, available_periods))
        df['atr'] = ta.atr(df['high'], df['low'], df['close'], length=min(14, available_periods))
        
        # Create a simple signal based on SMA crossover and RSI
        buy_condition = (
            (df['close'] > df['sma20']) &  # Price above SMA20
            (df['sma20'] > df['sma50']) &  # SMA20 above SMA50
            (df['rsi'] > 40) &             # RSI above oversold but not overbought
            (df['rsi'] < 70)
        )
        
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
        stop_loss = current_price - (current_atr * 2)
        take_profit = current_price + (current_atr * 4)
        
        # Get account balance and calculate position size
        balance = await binance_client.get_account_balance() or 0.0
        
        logger.info(f"Analysis of {symbol} at {interval}:")
        logger.info(f"Current price: ${current_price:.2f}")
        logger.info(f"ATR: ${current_atr:.2f}")
        logger.info(f"Signal: {'BUY' if buy_signal else 'SELL' if sell_signal else 'NONE'}")
        
        if buy_signal:
            position_size = risk_manager.calculate_position_size(
                account_balance=balance,
                entry_price=current_price,
                stop_loss=stop_loss,
                symbol=symbol
            )
            
            logger.info(f"Suggested entry: ${current_price:.2f}")
            logger.info(f"Suggested stop loss: ${stop_loss:.2f}")
            logger.info(f"Suggested take profit: ${take_profit:.2f}")
            logger.info(f"Suggested position size: {position_size:.8f} units (${position_size * current_price:.2f})")
        
        # Close connection
        await binance_client.close()
        
    except Exception as e:
        logger.error(f"Error in adaptive strategy test: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())

if __name__ == "__main__":
    import sys
    
    symbol = sys.argv[1] if len(sys.argv) > 1 else "BTCUSDT"
    interval = sys.argv[2] if len(sys.argv) > 2 else "4h"
    
    asyncio.run(adaptive_strategy_test(symbol, interval))