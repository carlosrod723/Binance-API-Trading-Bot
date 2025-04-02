import pandas as pd
import numpy as np
import pandas_ta as ta
from typing import Dict, Tuple, Optional, List, Union
import logging
from .base_strategy import Strategy

logger = logging.getLogger(__name__)

class EnhancedMACDStrategy(Strategy):
    """
    Enhanced MACD strategy with multiple timeframe analysis and volume confirmation.
    
    This strategy improves upon the basic MACD by:
    1. Using multiple timeframe confirmation
    2. Adding volume analysis
    3. Including trend filters
    4. Using dynamic take-profit and stop-loss
    """

    def __init__(self, fast_period: int = 12, slow_period: int = 26, signal_period: int = 9,
                 volume_factor: float = 1.5, rsi_period: int = 14, 
                 rsi_overbought: int = 70, rsi_oversold: int = 30):
        """
        Initialize the strategy.
        
        Args:
            fast_period: Fast period for MACD calculation (default: 12)
            slow_period: Slow period for MACD calculation (default: 26)
            signal_period: Signal period for MACD calculation (default: 9)
            volume_factor: Volume confirmation factor (default: 1.5)
            rsi_period: Period for RSI calculation (default: 14)
            rsi_overbought: RSI overbought threshold (default: 70)
            rsi_oversold: RSI oversold threshold (default: 30)
        """
        super().__init__()
        self.validate_parameters({
            "fast_period": fast_period,
            "slow_period": slow_period,
            "signal_period": signal_period,
            "volume_factor": volume_factor,
            "rsi_period": rsi_period,
            "rsi_overbought": rsi_overbought,
            "rsi_oversold": rsi_oversold
        })
        
        self.fast_period = fast_period
        self.slow_period = slow_period
        self.signal_period = signal_period
        self.volume_factor = volume_factor
        self.rsi_period = rsi_period
        self.rsi_overbought = rsi_overbought
        self.rsi_oversold = rsi_oversold
        
        logger.info(f"EnhancedMACDStrategy initialized: fast={fast_period}, slow={slow_period}, "
                    f"signal={signal_period}, volume_factor={volume_factor}")

    def validate_parameters(self, params: Dict) -> None:
        """Validate strategy parameters."""
        super().validate_parameters(params)
        
        for param_name in ["fast_period", "slow_period", "signal_period", "rsi_period"]:
            if not isinstance(params.get(param_name), int) or params[param_name] <= 0:
                raise ValueError(f"{param_name} must be a positive integer")
        
        if params["slow_period"] <= params["fast_period"]:
            raise ValueError("slow_period must be greater than fast_period")
            
        if not isinstance(params.get("volume_factor"), (int, float)) or params["volume_factor"] <= 0:
            raise ValueError("volume_factor must be a positive number")
            
        for param_name in ["rsi_overbought", "rsi_oversold"]:
            if not isinstance(params.get(param_name), int) or not 0 <= params[param_name] <= 100:
                raise ValueError(f"{param_name} must be an integer between 0 and 100")
                
        if params["rsi_oversold"] >= params["rsi_overbought"]:
            raise ValueError("rsi_oversold must be less than rsi_overbought")
            
        logger.debug("EnhancedMACD parameters validated: %s", params)

    def calculate_signals(self, data: pd.DataFrame) -> Tuple[bool, bool]:
        """
        Calculate buy/sell signals based on enhanced MACD strategy.
        
        Args:
            data: DataFrame with OHLCV data
            
        Returns:
            Tuple of (buy_signal, sell_signal)
        """
        try:
            self.validate_data(data)
        except ValueError as e:
            logger.error("Invalid data for MACD signals: %s", str(e))
            return (False, False)

        # Ensure sufficient data
        min_length = max(self.slow_period + self.signal_period, 50, self.rsi_period) + 10
        if len(data) < min_length:
            logger.warning(f"Insufficient data length ({len(data)}) for MACD calculation (need {min_length})")
            return (False, False)

        try:
            # Create copy to avoid SettingWithCopyWarning
            df = data.copy()
            
            # Calculate MACD
            macd_result = df.ta.macd(close=df['close'], 
                                    fast=self.fast_period, 
                                    slow=self.slow_period, 
                                    signal=self.signal_period)
            
            # Extract MACD components
            macd = macd_result[f'MACD_{self.fast_period}_{self.slow_period}_{self.signal_period}']
            macdsignal = macd_result[f'MACDs_{self.fast_period}_{self.slow_period}_{self.signal_period}']
            macdhist = macd_result[f'MACDh_{self.fast_period}_{self.slow_period}_{self.signal_period}']
            
            # Calculate additional indicators
            rsi = df.ta.rsi(length=self.rsi_period)
            ema50 = df.ta.ema(length=50)
            ema200 = df.ta.ema(length=200)
            
            # Calculate volume metrics
            avg_volume = df['volume'].rolling(window=20).mean()
            
            # Calculate ATR for stop loss
            atr = df.ta.atr(length=14)
            
            # Store indicators
            self.set_indicator("macd", macd)
            self.set_indicator("macdsignal", macdsignal)
            self.set_indicator("macdhist", macdhist)
            self.set_indicator("rsi", rsi)
            self.set_indicator("ema50", ema50)
            self.set_indicator("ema200", ema200)
            self.set_indicator("avg_volume", avg_volume)
            self.set_indicator("atr", atr)
            
            # Check for NaN values in latest data
            latest_idx = -1
            if any(pd.isna(x.iloc[latest_idx]) for x in [macd, macdsignal, rsi, ema50, ema200, avg_volume, atr]):
                logger.debug("Indicators not ready (NaN detected)")
                return (False, False)
            
            # Get latest values
            current_close = df['close'].iloc[latest_idx]
            current_volume = df['volume'].iloc[latest_idx]
            
            # Check for volume surge
            volume_surge = current_volume > avg_volume.iloc[latest_idx] * self.volume_factor
            
            # Check for trend
            uptrend = current_close > ema50.iloc[latest_idx] > ema200.iloc[latest_idx]
            downtrend = current_close < ema50.iloc[latest_idx] < ema200.iloc[latest_idx]
            
            # Check for MACD bullish crossover and histogram confirmation
            macd_buy_signal = (
                macd.iloc[latest_idx] > macdsignal.iloc[latest_idx] and 
                macd.iloc[latest_idx-1] <= macdsignal.iloc[latest_idx-1] and
                macdhist.iloc[latest_idx] > 0
            )
            
            # Check for MACD bearish crossover and histogram confirmation
            macd_sell_signal = (
                macd.iloc[latest_idx] < macdsignal.iloc[latest_idx] and 
                macd.iloc[latest_idx-1] >= macdsignal.iloc[latest_idx-1] and
                macdhist.iloc[latest_idx] < 0
            )
            
            # Check for RSI conditions
            rsi_oversold = rsi.iloc[latest_idx] < self.rsi_oversold
            rsi_overbought = rsi.iloc[latest_idx] > self.rsi_overbought
            
            # Generate final signals with all confirmations
            buy_signal = (
                macd_buy_signal and 
                uptrend and 
                (rsi_oversold or (rsi.iloc[latest_idx] < 60)) and
                volume_surge and 
                not self.is_holding_position
            )
            
            sell_signal = (
                (macd_sell_signal and downtrend) or
                rsi_overbought or
                (self.is_holding_position and 
                 current_close < ema50.iloc[latest_idx] and 
                 macdhist.iloc[latest_idx] < macdhist.iloc[latest_idx-1])
            )
            
            # If we generate a buy signal, also calculate stop loss and take profit
            if buy_signal:
                stop_loss = current_close - (atr.iloc[latest_idx] * 2)
                take_profit = current_close + (atr.iloc[latest_idx] * 4)
                
                self.set_indicator("stop_loss", stop_loss)
                self.set_indicator("take_profit", take_profit)
                
                logger.info(f"Enhanced MACD buy signal at {current_close:.2f}, "
                           f"SL: {stop_loss:.2f}, TP: {take_profit:.2f}")
            
            elif sell_signal:
                logger.info(f"Enhanced MACD sell signal at {current_close:.2f}")
            
            return (buy_signal, sell_signal)
            
        except Exception as e:
            logger.error(f"Error calculating MACD signals: {str(e)}")
            return (False, False)