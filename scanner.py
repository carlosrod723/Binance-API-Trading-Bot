# Create scanner.py
import asyncio
import pandas as pd
import numpy as np
import logging
from app.utils.binance_utils import BinanceClient
from app.config import config

# Cache directory
CACHE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'cache')
os.makedirs(CACHE_DIR, exist_ok=True)

async def get_cached_klines(client, symbol, interval, limit):
    """Get klines with caching to reduce API calls."""
    cache_file = os.path.join(CACHE_DIR, f"{symbol}_{interval}_klines.json")
    current_time = time.time()
    
    # Determine cache validity duration based on interval
    cache_duration = 60  # Default 1 minute
    if interval.endswith('m'):
        # For minute intervals, cache for half the interval time
        mins = int(interval[:-1])
        cache_duration = max(30, mins * 30)  # At least 30 seconds
    elif interval.endswith('h'):
        # For hour intervals, cache for 15 minutes
        cache_duration = 15 * 60
    elif interval in ['1d', '3d', '1w', '1M']:
        # For day/week/month intervals, cache for 1 hour
        cache_duration = 60 * 60
    
    # Try to load from cache first
    try:
        if os.path.exists(cache_file):
            file_age = current_time - os.path.getmtime(cache_file)
            
            # If cache is still valid
            if file_age < cache_duration:
                with open(cache_file, 'r') as f:
                    cached_data = json.load(f)
                logger.debug(f"Using cached data for {symbol} ({file_age:.1f}s old)")
                return cached_data
    except Exception as e:
        logger.warning(f"Error reading cache for {symbol}: {str(e)}")
    
    # If cache doesn't exist or is expired, fetch fresh data
    await asyncio.sleep(0.25)  # Add delay between API calls
    logger.debug(f"Fetching fresh data for {symbol}")
    klines = await client.get_klines(symbol, interval, limit)
    
    # Save to cache
    if klines:
        try:
            with open(cache_file, 'w') as f:
                json.dump(klines, f)
        except Exception as e:
            logger.warning(f"Error writing cache for {symbol}: {str(e)}")
    
    return klines

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def calculate_sma(series, length):
    """Calculate Simple Moving Average without pandas_ta."""
    return series.rolling(window=length).mean()

def calculate_rsi(series, length=14):
    """Calculate RSI without pandas_ta."""
    # Calculate price changes
    delta = series.diff()
    
    # Separate gains and losses
    gains = delta.copy()
    losses = delta.copy()
    gains[gains < 0] = 0
    losses[losses > 0] = 0
    losses = losses.abs()
    
    # Calculate average gains and losses
    avg_gain = gains.rolling(window=length).mean()
    avg_loss = losses.rolling(window=length).mean()
    
    # Calculate RS (Relative Strength)
    rs = avg_gain / avg_loss
    
    # Calculate RSI
    rsi = 100 - (100 / (1 + rs))
    return rsi

def calculate_atr(high, low, close, length=14):
    """Calculate Average True Range without pandas_ta."""
    # Calculate True Range
    tr1 = high - low
    tr2 = (high - close.shift(1)).abs()
    tr3 = (low - close.shift(1)).abs()
    
    # Get maximum of the three
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    
    # Calculate ATR
    atr = tr.rolling(window=length).mean()
    return atr

async def scan_market(interval="1h", symbols=None):
    """Scan multiple cryptocurrencies for trading opportunities with rate limiting and caching."""
    if symbols is None:
        # Default list of popular coins
        symbols = [
            "BTCUSDT", "ETHUSDT", "SOLUSDT", "AVAXUSDT", "DOGEUSDT", 
            "ADAUSDT", "XRPUSDT", "DOTUSDT", "LINKUSDT", "BNBUSDT",
            "LTCUSDT", "ATOMUSDT", "UNIUSDT", "AAVEUSDT", "SUSHIUSDT"
        ]
    
    # Rate limiting
    MAX_CALLS_PER_MINUTE = 30
    calls_made = 0
    minute_start = time.time()
    
    try:
        # Initialize client
        api_key, api_secret = config.get_binance_credentials()
        binance_client = BinanceClient(api_key, api_secret, testnet=config.ENV_MODE == "testnet")
        await binance_client.initialize()
        
        # Track opportunities
        opportunities = []
        
        # Get all symbols at once if possible
        all_symbols = None
        if hasattr(binance_client, "get_all_trading_pairs"):
            try:
                # Use rate limiting
                calls_made += 1
                if calls_made >= MAX_CALLS_PER_MINUTE:
                    wait_time = 60 - (time.time() - minute_start)
                    if wait_time > 0:
                        logger.info(f"Rate limiting - waiting {wait_time:.1f}s")
                        await asyncio.sleep(wait_time)
                    minute_start = time.time()
                    calls_made = 0
                
                all_symbols = await binance_client.get_all_trading_pairs()
                logger.info(f"Retrieved {len(all_symbols)} trading pairs")
                
                # Filter to ensure our symbols are valid
                valid_symbols = set(s['symbol'] for s in all_symbols)
                symbols = [s for s in symbols if s in valid_symbols]
                
            except Exception as e:
                logger.warning(f"Error getting all trading pairs: {str(e)}")
        
        # Process each symbol
        for symbol in symbols:
            try:
                logger.info(f"Scanning {symbol}...")
                
                # Rate limiting
                calls_made += 1
                if calls_made >= MAX_CALLS_PER_MINUTE:
                    wait_time = 60 - (time.time() - minute_start)
                    if wait_time > 0:
                        logger.info(f"Rate limiting - waiting {wait_time:.1f}s")
                        await asyncio.sleep(wait_time)
                    minute_start = time.time()
                    calls_made = 0
                
                # Get klines data with caching
                klines = await get_cached_klines(binance_client, symbol, interval, 100)
                if not klines or len(klines) < 50:
                    logger.warning(f"Not enough data for {symbol}")
                    continue
                
                # Process data
                df = pd.DataFrame(klines, columns=[
                    'timestamp', 'open', 'high', 'low', 'close', 'volume',
                    'close_time', 'quote_asset_volume', 'number_of_trades',
                    'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume', 'ignore'
                ])
                
                # Convert to numeric
                for col in ['open', 'high', 'low', 'close', 'volume']:
                    df[col] = pd.to_numeric(df[col])
                
                # Calculate basic indicators using our efficient functions
                df['sma20'] = calculate_sma(df['close'], 20)
                df['sma50'] = calculate_sma(df['close'], 50)
                df['rsi'] = calculate_rsi(df['close'], 14)
                df['atr'] = calculate_atr(df['high'], df['low'], df['close'], 14)
                
                # Calculate volume ratio
                df['vol_ratio'] = df['volume'] / df['volume'].rolling(10).mean()
                
                # Get latest values
                current_price = float(df['close'].iloc[-1])
                sma20 = float(df['sma20'].iloc[-1])
                sma50 = float(df['sma50'].iloc[-1])
                rsi = float(df['rsi'].iloc[-1])
                vol_ratio = float(df['vol_ratio'].iloc[-1])
                
                # Skip scoring if data is invalid
                if pd.isna(sma20) or pd.isna(sma50) or pd.isna(rsi):
                    logger.warning(f"Invalid indicator values for {symbol}")
                    continue
                
                # Score each asset (0-100)
                trend_score = 0
                if current_price > sma20:
                    trend_score += 25
                if sma20 > sma50:
                    trend_score += 25
                
                # RSI score - highest at 50 (balanced momentum)
                rsi_score = 100 - abs(rsi - 50) * 2
                
                # Volume score
                vol_score = min(100, vol_ratio * 50)
                
                # Recent performance score
                perf_1d = (current_price / float(df['close'].iloc[-24])) - 1 if len(df) >= 24 else 0
                perf_score = 50 + (perf_1d * 500)  # Scale percentage to score
                
                # Weighted total score
                total_score = (trend_score * 0.4) + (rsi_score * 0.3) + (vol_score * 0.1) + (perf_score * 0.2)
                
                # Create opportunity record
                opportunity = {
                    'symbol': symbol,
                    'price': current_price,
                    'trend_score': trend_score,
                    'rsi': rsi,
                    'rsi_score': rsi_score,
                    'vol_ratio': vol_ratio,
                    'vol_score': vol_score,
                    'perf_1d': perf_1d * 100,  # Convert to percentage
                    'perf_score': perf_score,
                    'total_score': total_score,
                    'buy_signal': current_price > sma20 and sma20 > sma50 and 40 < rsi < 70
                }
                
                opportunities.append(opportunity)
                
            except Exception as e:
                logger.error(f"Error scanning {symbol}: {str(e)}")
                continue
        
        # Close client
        await binance_client.close()
        
        # Sort by total score
        opportunities.sort(key=lambda x: x['total_score'], reverse=True)
        
        # Save all results to a file for reference
        try:
            with open(os.path.join(CACHE_DIR, 'latest_scan_results.json'), 'w') as f:
                json.dump(opportunities, f, indent=2)
        except Exception as e:
            logger.warning(f"Error saving scan results: {str(e)}")
            
        # Print results
        logger.info("\n--- TOP TRADING OPPORTUNITIES ---")
        for i, opp in enumerate(opportunities[:10]):
            logger.info(f"{i+1}. {opp['symbol']} - Score: {opp['total_score']:.1f} - Price: ${opp['price']:.2f} - RSI: {opp['rsi']:.1f} - 24h Change: {opp['perf_1d']:.1f}% - BUY Signal: {opp['buy_signal']}")
        
        logger.info("\n--- ASSETS WITH BUY SIGNALS ---")
        buy_signals = [opp for opp in opportunities if opp['buy_signal']]
        if buy_signals:
            for i, opp in enumerate(buy_signals):
                logger.info(f"{i+1}. {opp['symbol']} - Score: {opp['total_score']:.1f} - Price: ${opp['price']:.2f} - RSI: {opp['rsi']:.1f}")
        else:
            logger.info("No buy signals detected in current market conditions.")
            
        return opportunities
        
    except Exception as e:
        logger.error(f"Error in market scan: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return []

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Scan cryptocurrency market for trading opportunities')
    parser.add_argument('--interval', type=str, default="1h", help='Timeframe (15m, 30m, 1h, 4h)')
    parser.add_argument('--symbols', type=str, help='Comma-separated list of symbols to scan')
    parser.add_argument('--cache-only', action='store_true', help='Use cached data only, no API calls')
    parser.add_argument('--refresh-cache', action='store_true', help='Force refresh all cached data')
    
    args = parser.parse_args()
    
    # Parse symbols if provided
    symbol_list = None
    if args.symbols:
        symbol_list = [s.strip().upper() for s in args.symbols.split(',')]
    
    # If cache-only mode is enabled, modify the get_cached_klines function
    if args.cache_only:
        async def get_cached_klines_override(client, symbol, interval, limit):
            cache_file = os.path.join(CACHE_DIR, f"{symbol}_{interval}_klines.json")
            if os.path.exists(cache_file):
                with open(cache_file, 'r') as f:
                    return json.load(f)
            else:
                logger.warning(f"No cache found for {symbol}, skipping")
                return []
        
        # Override the function
        get_cached_klines = get_cached_klines_override
    
    # If refresh-cache is enabled, delete all cache files first
    if args.refresh_cache:
        for file in os.listdir(CACHE_DIR):
            if file.endswith('_klines.json'):
                try:
                    os.remove(os.path.join(CACHE_DIR, file))
                    logger.info(f"Deleted cache file: {file}")
                except Exception as e:
                    logger.warning(f"Failed to delete cache file {file}: {str(e)}")
    
    asyncio.run(scan_market(args.interval, symbol_list))