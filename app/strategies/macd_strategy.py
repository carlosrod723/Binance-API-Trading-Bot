import pandas as pd
import pandas_ta as ta  # Replace talib with pandas_ta
from typing import Tuple
from .base_strategy import Strategy
import logging

logger = logging.getLogger(__name__)

class MACDStrategy(Strategy):
    """MACD-based trading strategy with trend filter, volume confirmation, and profitability."""

    def __init__(self, fastperiod: int = 5, slowperiod: int = 13, signalperiod: int = 9, trend_period: int = 50):
        """Initialize with MACD periods and trend filter."""
        super().__init__()
        self.validate_parameters({"fastperiod": fastperiod, "slowperiod": slowperiod, "signalperiod": signalperiod, "trend_period": trend_period})
        self.fastperiod = fastperiod
        self.slowperiod = slowperiod
        self.signalperiod = signalperiod
        self.trend_period = trend_period
        logger.info("MACDStrategy initialized: fast=%d, slow=%d, signal=%d, trend=%d", fastperiod, slowperiod, signalperiod, trend_period)

    def validate_parameters(self, params: dict) -> None:
        """Validate MACD and trend parameters."""
        super().validate_parameters(params)
        for key in ["fastperiod", "slowperiod", "signalperiod", "trend_period"]:
            if not isinstance(params.get(key), int) or params[key] <= 0:
                raise ValueError(f"{key} must be a positive integer")
        if params["slowperiod"] <= params["fastperiod"]:
            raise ValueError("slowperiod must be greater than fastperiod")
        logger.debug("MACD parameters validated: %s", params)

    def calculate_signals(self, data: pd.DataFrame) -> Tuple[bool, bool]:
        """Calculate signals with MACD crossover, histogram confirmation, trend filter, and volume."""
        try:
            self.validate_data(data)
        except ValueError as e:
            logger.error("Invalid data for MACD signals: %s", str(e))
            return (False, False)

        close = data["close"]
        volume = data["volume"]
        min_length = max(self.slowperiod + self.signalperiod + 1, self.trend_period + 1, 20 + 1)  # Include volume SMA
        if len(close) < min_length:
            logger.warning("Insufficient data length (%d) for MACD+trend (need %d)", len(close), min_length)
            return (False, False)

        try:
            # Create a copy of data to avoid SettingWithCopyWarning
            df = data.copy()
            
            # Calculate MACD using pandas_ta
            macd_result = df.ta.macd(close=close, fast=self.fastperiod, slow=self.slowperiod, signal=self.signalperiod)
            # Extract MACD components - column names will be different from TA-Lib
            macd = macd_result[f'MACD_{self.fastperiod}_{self.slowperiod}_{self.signalperiod}']
            macdsignal = macd_result[f'MACDs_{self.fastperiod}_{self.slowperiod}_{self.signalperiod}']
            macdhist = macd_result[f'MACDh_{self.fastperiod}_{self.slowperiod}_{self.signalperiod}']
            
            # Calculate EMA for trend using pandas_ta
            ema200 = df.ta.ema(close=close, length=self.trend_period)
            
            # Calculate SMA for volume using pandas_ta
            avg_volume = df.ta.sma(close=volume, length=20)

            # Store indicators
            self.set_indicator("macd", macd)
            self.set_indicator("macdsignal", macdsignal)
            self.set_indicator("macdhist", macdhist)
            self.set_indicator("ema200", ema200)
            self.set_indicator("avg_volume", avg_volume)

            # Check for valid values
            if any(pd.isna(x) for x in [macd.iloc[-1], macdsignal.iloc[-1], macdhist.iloc[-1], 
                                        macd.iloc[-2], macdsignal.iloc[-2], 
                                        ema200.iloc[-1], avg_volume.iloc[-1]]):
                logger.debug("Indicators not ready (NaN detected)")
                return (False, False)

            # Volume confirmation
            volume_spike = volume.iloc[-1] > avg_volume.iloc[-1] * 1.5  # 50% above average

            # Generate signals
            buy_signal = (macd.iloc[-1] > macdsignal.iloc[-1] and macd.iloc[-2] <= macdsignal.iloc[-2] and 
                          macdhist.iloc[-1] > 0 and close.iloc[-1] > ema200.iloc[-1] and 
                          not self.is_holding_position and volume_spike)
            sell_signal = (macd.iloc[-1] < macdsignal.iloc[-1] and macd.iloc[-2] >= macdsignal.iloc[-2] and 
                           macdhist.iloc[-1] < 0 and close.iloc[-1] < ema200.iloc[-1] and 
                           self.is_holding_position and volume_spike)

            if buy_signal:
                logger.info("MACD buy signal detected at index %d", len(close) - 1)
            elif sell_signal:
                logger.info("MACD sell signal detected at index %d", len(close) - 1)
            else:
                logger.debug("No MACD signal at index %d", len(close) - 1)

            return (buy_signal, sell_signal)

        except Exception as e:
            logger.error("Error calculating MACD signals: %s", str(e))
            return (False, False)