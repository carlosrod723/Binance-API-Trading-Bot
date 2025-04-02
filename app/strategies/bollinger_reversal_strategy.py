import pandas as pd
import numpy as np
import pandas_ta as ta
from typing import Dict, Tuple, Optional, List, Union
import logging
from .base_strategy import Strategy

logger = logging.getLogger(__name__)

class BollingerReversalStrategy(Strategy):
    """
    Mean reversion strategy using Bollinger Bands and RSI for ranging markets.
    
    This strategy:
    1. Identifies overbought/oversold conditions with Bollinger Bands
    2. Confirms reversal with RSI and candlestick patterns
    3. Enters trades with good risk/reward when price reverts to mean
    """

    def __init__(self, length: int = 20, std_dev: float = 2.0, 
                rsi_period: int = 14, oversold: int = 30, overbought: int = 70,
                minimum_band_width: float = 1.5):
        """
        Initialize the strategy.
        
        Args:
            length: Period for Bollinger Bands calculation (default: 20)
            std_dev: Standard deviation multiplier for bands (default: 2.0)
            rsi_period: Period for RSI calculation (default: 14)
            oversold: RSI oversold threshold (default: 30)
            overbought: RSI overbought threshold (default: 70)
            minimum_band_width: Minimum width for valid signals (% of price) (default: 1.5)
        """
        super().__init__()
        self.validate_parameters({
            "length": length,
            "std_dev": std_dev,
            "rsi_period": rsi_period,
            "oversold": oversold,
            "overbought": overbought,
            "minimum_band_width": minimum_band_width
        })
        
        self.length = length
        self.std_dev = std_dev
        self.rsi_period = rsi_period
        self.oversold = oversold
        self.overbought = overbought
        self.minimum_band_width = minimum_band_width
        
        logger.info(f"BollingerReversalStrategy initialized: length={length}, std_dev={std_dev}, "
                   f"rsi_period={rsi_period}, oversold={oversold}, overbought={overbought}")

    def validate_parameters(self, params: Dict) -> None:
        """Validate strategy parameters."""
        super().validate_parameters(params)
        
        for param_name in ["length", "rsi_period"]:
            if not isinstance(params.get(param_name), int) or params[param_name] <= 0:
                raise ValueError(f"{param_name} must be a positive integer")
        
        for param_name in ["std_dev", "minimum_band_width"]:
            if not isinstance(params.get(param_name), (int, float)) or params[param_name] <= 0:
                raise ValueError(f"{param_name} must be a positive number")
        
        for param_name in ["oversold", "overbought"]:
            if not isinstance(params.get(param_name), int) or not 0 <= params[param_name] <= 100:
                raise ValueError(f"{param_name} must be an integer between 0 and 100")
        
        if params["oversold"] >= params["overbought"]:
            raise ValueError("oversold must be less than overbought")
        
        logger.debug("BollingerReversal parameters validated: %s", params)

    def calculate_signals(self, data: pd.DataFrame) -> Tuple[bool, bool]:
        """
        Calculate buy/sell signals based on Bollinger Band reversals.
        
        Args:
            data: DataFrame with OHLCV data
            
        Returns:
            Tuple of (buy_signal, sell_signal)
        """
        try:
            self.validate_data(data)
        except ValueError as e:
            logger.error("Invalid data for Bollinger signals: %s", str(e))
            return (False, False)

        # Ensure sufficient data
        min_length = max(self.length, self.rsi_period) + 10
        if len(data) < min_length:
            logger.warning(f"Insufficient data length ({len(data)}) for Bollinger calculation (need {min_length})")
            return (False, False)

        try:
            # Create copy to avoid SettingWithCopyWarning
            df = data.copy()
            
            # Calculate Bollinger Bands
            bbands = df.ta.bbands(length=self.length, std=self.std_dev)
            bbands_upper = bbands[f'BBU_{self.length}_{self.std_dev}']
            bbands_middle = bbands[f'BBM_{self.length}_{self.std_dev}']
            bbands_lower = bbands[f'BBL_{self.length}_{self.std_dev}']
            
            # Calculate percentage bandwidth
            bandwidth = ((bbands_upper - bbands_lower) / bbands_middle) * 100
            
            # Calculate RSI
            rsi = df.ta.rsi(length=self.rsi_period)
            
            # Calculate stochastic for additional confirmation
            stoch = df.ta.stoch(high=df['high'], low=df['low'], close=df['close'], k=14, d=3, smooth_k=3)
            stoch_k = stoch['STOCHk_14_3_3']
            stoch_d = stoch['STOCHd_14_3_3']
            
            # Calculate MACD for trend direction
            macd_result = df.ta.macd(close=df['close'], fast=12, slow=26, signal=9)
            macd = macd_result[f'MACD_12_26_9']
            macdsignal = macd_result[f'MACDs_12_26_9']
            
            # Store indicators
            self.set_indicator("bbands_upper", bbands_upper)
            self.set_indicator("bbands_middle", bbands_middle)
            self.set_indicator("bbands_lower", bbands_lower)
            self.set_indicator("bandwidth", bandwidth)
            self.set_indicator("rsi", rsi)
            self.set_indicator("stoch_k", stoch_k)
            self.set_indicator("stoch_d", stoch_d)
            self.set_indicator("macd", macd)
            self.set_indicator("macdsignal", macdsignal)
            
            # Check for NaN values in latest data
            latest_idx = -1
            if any(pd.isna(x.iloc[latest_idx]) for x in [
                bbands_upper, bbands_middle, bbands_lower, bandwidth, rsi, stoch_k, stoch_d
            ]):
                logger.debug("Indicators not ready (NaN detected)")
                return (False, False)
            
            # Get latest values
            current_close = df['close'].iloc[latest_idx]
            current_high = df['high'].iloc[latest_idx]
            current_low = df['low'].iloc[latest_idx]
            
            # Check for bounce from lower band
            lower_band_touch = current_low <= bbands_lower.iloc[latest_idx]
            bounce_from_low = (
                df['low'].iloc[latest_idx-1] <= bbands_lower.iloc[latest_idx-1] and
                current_close > df['open'].iloc[latest_idx]  # Bullish candle
            )
            
            # Check for bounce from upper band
            upper_band_touch = current_high >= bbands_upper.iloc[latest_idx]
            bounce_from_high = (
                df['high'].iloc[latest_idx-1] >= bbands_upper.iloc[latest_idx-1] and
                current_close < df['open'].iloc[latest_idx]  # Bearish candle
            )
            
            # Check RSI conditions
            rsi_oversold = rsi.iloc[latest_idx] < self.oversold
            rsi_overbought = rsi.iloc[latest_idx] > self.overbought
            
            # Check stochastic crossover conditions
            stoch_bullish = stoch_k.iloc[latest_idx] > stoch_d.iloc[latest_idx] and stoch_k.iloc[latest_idx-1] <= stoch_d.iloc[latest_idx-1]
            stoch_bearish = stoch_k.iloc[latest_idx] < stoch_d.iloc[latest_idx] and stoch_k.iloc[latest_idx-1] >= stoch_d.iloc[latest_idx-1]
            
            # Check for sufficient volatility
            sufficient_volatility = bandwidth.iloc[latest_idx] > self.minimum_band_width
            
            # Generate signals
            buy_signal = (
                (lower_band_touch or bounce_from_low) and
                rsi_oversold and
                stoch_bullish and
                sufficient_volatility and
                not self.is_holding_position
            )
            
            sell_signal = (
                (upper_band_touch or bounce_from_high) and
                rsi_overbought and
                stoch_bearish and
                sufficient_volatility and
                self.is_holding_position
            )
            
            # If buy signal, calculate stop loss and take profit
            if buy_signal:
                # Set stop just below the recent low
                stop_loss = min(df['low'].iloc[-3:]) * 0.995
                # Set take profit at middle band or upper band
                if current_close < bbands_middle.iloc[latest_idx]:
                    take_profit = bbands_middle.iloc[latest_idx]
                else:
                    take_profit = bbands_upper.iloc[latest_idx]
                
                self.set_indicator("stop_loss", stop_loss)
                self.set_indicator("take_profit", take_profit)
                
                logger.info(f"Bollinger reversal buy signal at {current_close:.2f}, "
                           f"SL: {stop_loss:.2f}, TP: {take_profit:.2f}")
            
            elif sell_signal:
                logger.info(f"Bollinger reversal sell signal at {current_close:.2f}")
            
            return (buy_signal, sell_signal)
            
        except Exception as e:
            logger.error(f"Error calculating Bollinger signals: {str(e)}")
            return (False, False)