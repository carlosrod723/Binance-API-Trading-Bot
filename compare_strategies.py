# compare_strategies.py in project root
import asyncio
import pandas as pd
import time
from app.utils.binance_utils import BinanceClient
from app.strategies.volatility_breakout_strategy import VolatilityBreakoutStrategy
from app.strategies.enhanced_macd_strategy import EnhancedMACDStrategy
from app.strategies.bollinger_reversal_strategy import BollingerReversalStrategy
from app.config import config
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def compare_strategies(symbols=["BTCUSDT", "ETHUSDT"], interval="15m"):
    """Compare different strategies across multiple symbols."""
    try:
        # Initialize client
        api_key, api_secret = config.get_binance_credentials()
        binance_client = BinanceClient(api_key, api_secret, testnet=config.ENV_MODE == "testnet")
        await binance_client.initialize()
        
        # Initialize strategies
        strategies = {
            "Volatility Breakout": VolatilityBreakoutStrategy(),
            "Enhanced MACD": EnhancedMACDStrategy(),
            "Bollinger Reversal": BollingerReversalStrategy()
        }
        
        for symbol in symbols:
            logger.info(f"Analyzing {symbol} at {interval} timeframe...")
            
            # Get historical data
            klines = await binance_client.get_klines(symbol, interval, 500)
            if not klines:
                logger.error(f"Failed to fetch data for {symbol}")
                continue
            
            # Convert to DataFrame
            df = pd.DataFrame(klines, columns=[
                'timestamp', 'open', 'high', 'low', 'close', 'volume',
                'close_time', 'quote_asset_volume', 'number_of_trades',
                'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume', 'ignore'
            ])
            
            # Convert types
            for col in ['open', 'high', 'low', 'close', 'volume']:
                df[col] = pd.to_numeric(df[col])
            
            # Check signals for each strategy
            for name, strategy in strategies.items():
                buy_signal, sell_signal = strategy.calculate_signals(df)
                signal = "BUY" if buy_signal else "SELL" if sell_signal else "NONE"
                logger.info(f"{name}: {signal} signal for {symbol}")
                
                # If buy signal, log stop loss and take profit
                if buy_signal:
                    indicators = strategy.get_indicator_values()
                    if 'stop_loss' in indicators and 'take_profit' in indicators:
                        current_price = float(df['close'].iloc[-1])
                        sl = indicators.get('stop_loss', 0)
                        tp = indicators.get('take_profit', 0)
                        if sl and tp:
                            risk = (current_price - sl) / current_price * 100
                            reward = (tp - current_price) / current_price * 100
                            logger.info(f"   Entry: {current_price:.2f}, SL: {sl:.2f} ({risk:.1f}%), TP: {tp:.2f} ({reward:.1f}%)")
            
            # Prevent hitting rate limits
            await asyncio.sleep(1)
        
        # Close connection
        await binance_client.close()
        
    except Exception as e:
        logger.error(f"Error comparing strategies: {str(e)}")

# Main section
if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Compare trading strategies')
    parser.add_argument('--symbols', type=str, default="BTCUSDT,ETHUSDT",
                        help='Comma-separated list of symbols to analyze')
    parser.add_argument('--interval', type=str, default="15m",
                        help='Timeframe interval (1m, 5m, 15m, 1h, 4h, 1d)')
    
    args = parser.parse_args()
    symbols = args.symbols.split(',')
    
    # Run strategy comparison
    asyncio.run(compare_strategies(symbols=symbols, interval=args.interval))