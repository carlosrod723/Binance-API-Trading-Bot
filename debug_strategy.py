import asyncio
import pandas as pd
import numpy as np
import logging
import traceback
from app.utils.binance_utils import BinanceClient
from app.strategies.volatility_breakout_strategy import VolatilityBreakoutStrategy
from app.config import config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def debug_strategy(symbol="BTCUSDT", interval="1h"):
    """Debug the volatility breakout strategy with step-by-step logging."""
    try:
        # Initialize client
        api_key, api_secret = config.get_binance_credentials()
        binance_client = BinanceClient(api_key, api_secret, testnet=config.ENV_MODE == "testnet")
        await binance_client.initialize()
        
        # Get historical data
        klines = await binance_client.get_klines(symbol, interval, 500)
        
        # Convert to DataFrame with explicit types
        df = pd.DataFrame(klines, columns=[
            'timestamp', 'open', 'high', 'low', 'close', 'volume',
            'close_time', 'quote_asset_volume', 'number_of_trades',
            'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume', 'ignore'
        ])
        
        # Convert ALL numeric columns to float
        numeric_cols = ['open', 'high', 'low', 'close', 'volume', 
                      'quote_asset_volume', 'taker_buy_base_asset_volume', 
                      'taker_buy_quote_asset_volume']
        
        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # Print column types to verify
        for col in df.columns:
            logger.info(f"Column {col} has type: {df[col].dtype}")
            logger.info(f"First value of {col}: {df[col].iloc[0]}")
        
        # Initialize strategy
        strategy = VolatilityBreakoutStrategy()
        
        # Calculate signals
        logger.info("Calculating signals...")
        try:
            buy_signal, sell_signal = strategy.calculate_signals(df)
            
            logger.info(f"Final result: Buy signal = {buy_signal}, Sell signal = {sell_signal}")
            
            # Print indicator values
            indicators = strategy.get_indicator_values()
            logger.info("Indicator values:")
            for name, value in indicators.items():
                logger.info(f"  {name}: {value}")
                
        except Exception as e:
            logger.error(f"Error during signal calculation: {str(e)}")
            logger.error(traceback.format_exc())
        
        # Close connection
        await binance_client.close()
        
    except Exception as e:
        logger.error(f"Error in debug: {str(e)}")
        logger.error(traceback.format_exc())

if __name__ == "__main__":
    asyncio.run(debug_strategy())