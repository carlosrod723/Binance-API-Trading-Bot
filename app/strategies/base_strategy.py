from abc import ABC, abstractmethod
import pandas as pd
from typing import Dict, Tuple, Union, Optional
import logging

logger = logging.getLogger(__name__)

class Strategy(ABC):
    """Base class for all trading strategies."""

    def __init__(self, indicators: Optional[Dict[str, Union[pd.Series, float, None]]] = None):
        """Initialize the strategy."""
        self.indicators = indicators if indicators is not None else {}
        self.is_holding_position = False  # Track position state
        
        # Add cache for signal calculations to reduce reprocessing
        self._signal_cache = {
            'last_timestamp': None,
            'buy_signal': False,
            'sell_signal': False
        }
        logger.debug("Strategy initialized with indicators: %s", self.indicators.keys())

    @abstractmethod
    def calculate_signals(self, data: pd.DataFrame) -> Tuple[bool, bool]:
        """Calculate buy/sell signals."""
        required_columns = {"timestamp", "open", "high", "low", "close", "volume"}
        if not data.empty:
            missing = required_columns - set(data.columns)
            if missing:
                logger.error("DataFrame missing required columns: %s", missing)
                raise ValueError(f"DataFrame missing required columns: {missing}")
                
            # Check if we can use cached results
            if not self.needs_recalculation(data):
                logger.debug("Using cached signals")
                return self._signal_cache['buy_signal'], self._signal_cache['sell_signal']
            
            # Update cache timestamp
            if 'timestamp' in data.columns:
                self._signal_cache['last_timestamp'] = data['timestamp'].iloc[-1]
        else:
            logger.error("Empty DataFrame provided to calculate_signals")
            raise ValueError("DataFrame is empty")
        
        raise NotImplementedError("Subclasses must implement calculate_signals()")

    def validate_parameters(self, params: Dict) -> None:
        """Validate strategy parameters."""
        if not isinstance(params, dict):
            raise ValueError("Parameters must be a dictionary")

    def get_indicator_values(self) -> Dict[str, Optional[float]]:
        """Retrieve latest indicator values."""
        values = {}
        for name, indicator in self.indicators.items():
            if indicator is None:
                values[name] = None
            elif isinstance(indicator, pd.Series):
                try:
                    last_value = float(indicator.iloc[-1])
                    values[name] = last_value if not pd.isna(last_value) else None
                except (IndexError, ValueError, AttributeError):
                    values[name] = None
                    logger.warning("Invalid or empty Series for indicator %s", name)
            elif isinstance(indicator, (int, float)):
                values[name] = float(indicator) if not pd.isna(indicator) else None
            else:
                values[name] = None
                logger.warning("Unsupported type for indicator %s: %s", name, type(indicator))
        logger.debug("Retrieved indicator values: %s", values)
        return values

    def set_indicator(self, name: str, value: Union[pd.Series, float, None]) -> None:
        """Set or update an indicator value."""
        if name in self.indicators:
            logger.debug("Updating existing indicator %s", name)
        self.indicators[name] = value
        logger.debug("Set indicator %s to value type %s", name, type(value))

    def update_signal_cache(self, buy_signal: bool, sell_signal: bool, timestamp=None) -> None:
        """Update the signal cache with latest results.
        
        Args:
            buy_signal: Whether a buy signal is present
            sell_signal: Whether a sell signal is present
            timestamp: Optional timestamp for the signals
        """
        self._signal_cache['buy_signal'] = buy_signal
        self._signal_cache['sell_signal'] = sell_signal
        if timestamp is not None:
            self._signal_cache['last_timestamp'] = timestamp
        logger.debug(f"Signal cache updated: buy={buy_signal}, sell={sell_signal}")

    def smooth_signals(self, 
                      buy_signal: bool, 
                      sell_signal: bool, 
                      min_consecutive: int = 1,
                      data: Optional[pd.DataFrame] = None) -> Tuple[bool, bool]:
        """Apply signal smoothing to reduce false signals.
        
        Args:
            buy_signal: Raw buy signal
            sell_signal: Raw sell signal
            min_consecutive: Minimum consecutive signals required
            data: Optional DataFrame to check historical signals
        
        Returns:
            Tuple of filtered buy and sell signals
        """
        # Simple case - no smoothing required
        if min_consecutive <= 1:
            return buy_signal, sell_signal
        
        # For more advanced smoothing, we need historical data
        if data is None or data.empty or len(data) < min_consecutive:
            return buy_signal, sell_signal
        
        # Override this in subclasses for more sophisticated filtering
        # This is just a basic implementation
        return buy_signal, sell_signal

    def is_duplicate_calculation(self, data: pd.DataFrame) -> bool:
        """Check if this is a duplicate calculation request for the same data.
        
        Args:
            data: DataFrame with OHLCV data
        
        Returns:
            bool: True if this appears to be a duplicate calculation
        """
        # If cache is empty, not a duplicate
        if self._signal_cache['last_timestamp'] is None:
            return False
        
        # Check timestamp and last candle values
        if not data.empty and len(data) > 0:
            if 'timestamp' in data.columns and 'close' in data.columns:
                latest_ts = data['timestamp'].iloc[-1]
                latest_close = data['close'].iloc[-1]
                
                # Check if we processed this exact timestamp and price before
                if (latest_ts == self._signal_cache['last_timestamp'] and
                    hasattr(self, '_last_processed_close') and
                    latest_close == self._last_processed_close):
                    logger.debug("Duplicate calculation detected - same timestamp and price")
                    return True
                
                # Update for next check
                self._last_processed_close = latest_close
        
        return False

    def needs_recalculation(self, data: pd.DataFrame) -> bool:
        """Determine if signal recalculation is needed based on new data.
        
        Args:
            data: DataFrame with OHLCV data
        
        Returns:
            bool: True if signals should be recalculated
        """
        # Always calculate if cache is empty
        if self._signal_cache['last_timestamp'] is None:
            return True
        
        # Check for duplicate calculation request
        if self.is_duplicate_calculation(data):
            return False
        
        # Check if we have new data since last calculation
        if not data.empty and 'timestamp' in data.columns:
            latest_timestamp = data['timestamp'].iloc[-1]
            if latest_timestamp != self._signal_cache['last_timestamp']:
                return True
        
        # Default to recalculation if we can't determine
        return True

    def validate_data(self, data: pd.DataFrame) -> bool:
        """Validate input data and return whether valid.
        
        Args:
            data: DataFrame with OHLCV data
        
        Returns:
            bool: True if data is valid
        """
        # Quick check for empty data
        if data is None or data.empty:
            logger.warning("Empty DataFrame provided")
            return False
        
        # Check required columns
        required_columns = {"timestamp", "open", "high", "low", "close", "volume"}
        missing = required_columns - set(data.columns)
        if missing:
            logger.warning(f"DataFrame missing required columns: {missing}")
            return False
        
        # Check for numeric price data
        if not pd.api.types.is_numeric_dtype(data["close"]):
            logger.warning("Close prices must be numeric")
            return False
        
        # Check for sufficient data points
        if len(data) < 30:  # Most strategies need at least 30 bars
            logger.warning(f"Insufficient data points: {len(data)}")
            return False
        
        return True

    def update_position(self, bought: bool, sold: bool) -> None:
        """Update position state after a trade."""
        if bought:
            self.is_holding_position = True
        elif sold:
            self.is_holding_position = False
        logger.debug("Position updated: holding=%s", self.is_holding_position)

      