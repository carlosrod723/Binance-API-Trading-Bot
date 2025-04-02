import pandas as pd
import numpy as np
import pandas_ta as ta
from typing import Dict, Tuple, Optional, List, Union
import logging
from .base_strategy import Strategy

logger = logging.getLogger(__name__)

class VolatilityBreakoutStrategy(Strategy):
    """ATR-based volatility breakout strategy that trades breakouts with proper risk management."""

    def __init__(self, atr_period: int = 14, breakout_period: int = 20, 
                 atr_multiplier: float = 2.0, volume_factor: float = 1.3):
        """Initialize the strategy with parameters."""
        super().__init__()
        self.validate_parameters({
            "atr_period": atr_period,
            "breakout_period": breakout_period,
            "atr_multiplier": atr_multiplier,
            "volume_factor": volume_factor
        })
        
        self.atr_period = atr_period
        self.breakout_period = breakout_period
        self.atr_multiplier = atr_multiplier
        self.volume_factor = volume_factor
        
        logger.info(f"VolatilityBreakoutStrategy initialized: atr_period={atr_period}, "
                   f"breakout_period={breakout_period}, atr_multiplier={atr_multiplier}")

    def validate_parameters(self, params: Dict) -> None:
        """Validate strategy parameters."""
        super().validate_parameters(params)
        
        if not isinstance(params.get("atr_period"), int) or params["atr_period"] <= 0:
            raise ValueError("atr_period must be a positive integer")
            
        if not isinstance(params.get("breakout_period"), int) or params["breakout_period"] <= 0:
            raise ValueError("breakout_period must be a positive integer")
            
        if not isinstance(params.get("atr_multiplier"), (int, float)) or params["atr_multiplier"] <= 0:
            raise ValueError("atr_multiplier must be a positive number")
            
        if not isinstance(params.get("volume_factor"), (int, float)) or params["volume_factor"] <= 0:
            raise ValueError("volume_factor must be a positive number")
            
        logger.debug("VolatilityBreakout parameters validated: %s", params)

    def calculate_signals(self, data: pd.DataFrame) -> Tuple[bool, bool]:
        """Calculate buy/sell signals based on volatility breakouts."""
        try:
            self.validate_data(data)
        except ValueError as e:
            logger.error("Invalid data for breakout signals: %s", str(e))
            return (False, False)

        # Ensure sufficient data
        min_length = max(self.atr_period, self.breakout_period, 50) + 10
        if len(data) < min_length:
            logger.warning(f"Insufficient data length ({len(data)}) for breakout calculation (need {min_length})")
            return (False, False)

        try:
            # Create copy to avoid SettingWithCopyWarning
            df = data.copy()
            
            # Ensure all data is numeric
            for col in ['open', 'high', 'low', 'close', 'volume']:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
                else:
                    logger.error(f"Required column {col} missing from DataFrame")
                    return (False, False)
            
            # Calculate ATR
            atr = df.ta.atr(length=self.atr_period)
            
            # Calculate highest high and lowest low for breakout period
            high_max = df['high'].rolling(window=self.breakout_period).max().shift(1)
            low_min = df['low'].rolling(window=self.breakout_period).min().shift(1)
            
            # Calculate average volume
            avg_volume = df['volume'].rolling(window=20).mean()
            
            # Calculate price momentum
            roc = df.ta.roc(close=df['close'], length=5)
            
            # Calculate trend direction with EMA
            # Extract Series from any potential DataFrame result
            ema50_result = df.ta.ema(close=df['close'], length=50)
            ema200_result = df.ta.ema(close=df['close'], length=200)
            
            # Handle various return types from pandas-ta
            if isinstance(ema50_result, pd.DataFrame):
                ema50 = ema50_result.iloc[:, 0]  # Get first column as Series
            else:
                ema50 = ema50_result
                
            if isinstance(ema200_result, pd.DataFrame):
                ema200 = ema200_result.iloc[:, 0]  # Get first column as Series
            else:
                ema200 = ema200_result
            
            # Store indicators
            self.set_indicator("atr", atr)
            self.set_indicator("high_max", high_max)
            self.set_indicator("low_min", low_min)
            self.set_indicator("avg_volume", avg_volume)
            self.set_indicator("roc", roc)
            self.set_indicator("ema50", ema50)
            self.set_indicator("ema200", ema200)
            
            # Get the latest index
            latest_idx = -1
            
            # Check for NaN values in latest data - one by one
            if (pd.isna(atr.iloc[latest_idx]) or 
                pd.isna(high_max.iloc[latest_idx]) or 
                pd.isna(low_min.iloc[latest_idx]) or 
                pd.isna(avg_volume.iloc[latest_idx]) or 
                pd.isna(ema50.iloc[latest_idx]) or 
                pd.isna(ema200.iloc[latest_idx]) or 
                pd.isna(roc.iloc[latest_idx])):
                logger.debug("Indicators not ready (NaN detected)")
                return (False, False)
            
            # Extract single scalar values from Series for comparison
            try:
                current_close = float(df['close'].iloc[latest_idx])
                current_high = float(df['high'].iloc[latest_idx])
                current_low = float(df['low'].iloc[latest_idx])
                current_volume = float(df['volume'].iloc[latest_idx])
                
                latest_atr = float(atr.iloc[latest_idx])
                latest_high_max = float(high_max.iloc[latest_idx])
                latest_low_min = float(low_min.iloc[latest_idx])
                latest_avg_volume = float(avg_volume.iloc[latest_idx])
                latest_ema50 = float(ema50.iloc[latest_idx])
                latest_ema200 = float(ema200.iloc[latest_idx])
                latest_roc = float(roc.iloc[latest_idx])
            except (ValueError, TypeError) as e:
                logger.warning(f"Value conversion error: {e}")
                return (False, False)
            
            # Now use scalar values for all comparisons
            volume_surge = current_volume > (latest_avg_volume * self.volume_factor)
            uptrend = latest_ema50 > latest_ema200
            downtrend = latest_ema50 < latest_ema200
            
            breakout_up = current_high > latest_high_max
            breakout_down = current_low < latest_low_min
            
            strong_momentum_up = latest_roc > 1.0
            strong_momentum_down = latest_roc < -1.0
            
            # Generate signals using scalar values
            buy_signal = (
                breakout_up and 
                volume_surge and 
                strong_momentum_up and 
                uptrend and 
                not self.is_holding_position
            )
            
            sell_signal = (
                (breakout_down and volume_surge and strong_momentum_down) or
                (downtrend and self.is_holding_position)
            )
            
            # If buy signal, calculate stop loss and take profit
            if buy_signal:
                stop_loss = current_close - (latest_atr * self.atr_multiplier)
                take_profit = current_close + (latest_atr * self.atr_multiplier * 2)
                
                self.set_indicator("stop_loss", stop_loss)
                self.set_indicator("take_profit", take_profit)
                
                logger.info(f"Breakout buy signal at {current_close:.2f}, "
                           f"SL: {stop_loss:.2f}, TP: {take_profit:.2f}")
            
            elif sell_signal:
                logger.info(f"Breakout sell signal at {current_close:.2f}")
            
            return (buy_signal, sell_signal)
            
        except Exception as e:
            logger.error(f"Error calculating breakout signals: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            return (False, False)