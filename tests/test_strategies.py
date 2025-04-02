import pytest
import pandas as pd
import numpy as np
from app.strategies.macd_strategy import MACDStrategy
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Test data with enough points for MACD (slow=13 + signal=9 + trend=50 + volume=20)
TREND_DATA = pd.DataFrame({
    "timestamp": pd.date_range(start="2023-01-01", periods=100, freq="5T"),
    "open": np.concatenate([np.linspace(100, 120, 50), np.linspace(120, 100, 50)]),  # Rise then fall
    "high": np.concatenate([np.linspace(105, 125, 50), np.linspace(125, 105, 50)]),
    "low": np.concatenate([np.linspace(95, 115, 50), np.linspace(115, 95, 50)]),
    "close": np.concatenate([np.linspace(100, 120, 50), np.linspace(120, 100, 50)]),
    "volume": np.concatenate([np.ones(49) * 1.0, [2.0], np.ones(49) * 1.0, [2.0]])  # Volume spike at peak and trough
})

@pytest.fixture
def macd_strategy() -> MACDStrategy:
    """Provide a default MACDStrategy instance."""
    return MACDStrategy(fastperiod=5, slowperiod=13, signalperiod=9, trend_period=50)

@pytest.fixture
def trend_data() -> pd.DataFrame:
    """Provide realistic trend data for MACD testing."""
    return TREND_DATA.copy()

def test_calculate_signals_trend_data(macd_strategy: MACDStrategy, trend_data: pd.DataFrame):
    """Test MACD signals with realistic trend data, including trend and volume filters."""
    # Initial state: not holding position
    macd_strategy.is_holding_position = False
    buy_signal, sell_signal = macd_strategy.calculate_signals(trend_data.iloc[:50])  # Rising trend
    logger.info(f"Rising trend - Buy: {buy_signal}, Sell: {sell_signal}")
    assert buy_signal is True, "Should detect buy signal on upward crossover with volume spike above EMA"
    assert sell_signal is False, "No sell signal expected during upward trend when not holding"

    macd_strategy.update_position(True, False)  # Simulate holding position after buy
    buy_signal, sell_signal = macd_strategy.calculate_signals(trend_data)  # Full trend with fall
    logger.info(f"Falling trend - Buy: {buy_signal}, Sell: {sell_signal}")
    assert sell_signal is True, "Should detect sell signal on downward crossover with volume spike below EMA"
    assert buy_signal is False, "No buy signal expected during downward trend when holding"

def test_calculate_signals_insufficient_data(macd_strategy: MACDStrategy):
    """Test signals with insufficient data (less than max required periods)."""
    short_data = pd.DataFrame({
        "timestamp": pd.date_range(start="2023-01-01", periods=20, freq="5T"),
        "open": [100] * 20,
        "high": [101] * 20,
        "low": [99] * 20,
        "close": [100] * 20,
        "volume": [1] * 20
    })
    buy_signal, sell_signal = macd_strategy.calculate_signals(short_data)
    assert buy_signal is False, "No buy signal with insufficient data"
    assert sell_signal is False, "No sell signal with insufficient data"
    logger.info("Correctly handled insufficient data")

def test_calculate_signals_custom_periods():
    """Test signals with custom MACD periods."""
    strategy = MACDStrategy(fastperiod=3, slowperiod=7, signalperiod=2, trend_period=10)
    data = pd.DataFrame({
        "timestamp": pd.date_range(start="2023-01-01", periods=30, freq="5T"),
        "open": np.concatenate([np.linspace(100, 110, 15), np.linspace(110, 100, 15)]),
        "high": np.concatenate([np.linspace(101, 111, 15), np.linspace(111, 101, 15)]),
        "low": np.concatenate([np.linspace(99, 109, 15), np.linspace(109, 99, 15)]),
        "close": np.concatenate([np.linspace(100, 110, 15), np.linspace(110, 100, 15)]),
        "volume": np.concatenate([np.ones(14) * 1.0, [2.0], np.ones(14) * 1.0, [2.0]])
    })
    strategy.is_holding_position = False
    buy_signal, sell_signal = strategy.calculate_signals(data.iloc[:15])  # Rising
    logger.info(f"Custom periods (rising) - Buy: {buy_signal}, Sell: {sell_signal}")
    assert buy_signal is True, "Should detect buy signal with custom periods on upward trend"
    assert sell_signal is False, "No sell signal expected when not holding"

    strategy.update_position(True, False)
    buy_signal, sell_signal = strategy.calculate_signals(data)  # Full trend
    logger.info(f"Custom periods (falling) - Buy: {buy_signal}, Sell: {sell_signal}")
    assert sell_signal is True, "Should detect sell signal with custom periods on downward trend"
    assert buy_signal is False, "No buy signal expected when holding"

def test_calculate_signals_nan_values(macd_strategy: MACDStrategy):
    """Test signals with NaN values in data."""
    nan_data = pd.DataFrame({
        "timestamp": pd.date_range(start="2023-01-01", periods=100, freq="5T"),
        "open": [100] * 100,
        "high": [101] * 100,
        "low": [99] * 100,
        "close": [100] * 50 + [np.nan] + [100] * 49,  # NaN in middle
        "volume": [1] * 100
    })
    buy_signal, sell_signal = macd_strategy.calculate_signals(nan_data)
    assert buy_signal is False, "No buy signal with NaN values"
    assert sell_signal is False, "No sell signal with NaN values"
    logger.info("Correctly handled NaN values")

def test_get_indicator_values(macd_strategy: MACDStrategy, trend_data: pd.DataFrame):
    """Test retrieval of indicator values."""
    macd_strategy.calculate_signals(trend_data)  # Populate indicators
    values = macd_strategy.get_indicator_values()
    logger.info(f"Indicator Values: {values}")
    assert isinstance(values, dict), "Indicator values should be a dict"
    expected_keys = {"macd", "macdsignal", "macdhist", "ema200", "avg_volume"}
    assert all(key in values for key in expected_keys), "Should include all expected indicators"
    for key, value in values.items():
        assert isinstance(value, (float, type(None))), f"{key} should be float or None"
        if value is not None:
            assert not pd.isna(value), f"{key} should not be NaN"

def test_invalid_periods():
    """Test instantiation with invalid MACD periods."""
    with pytest.raises(ValueError, match="fastperiod must be a positive integer"):
        MACDStrategy(fastperiod=-1)
    with pytest.raises(ValueError, match="slowperiod must be greater than fastperiod"):
        MACDStrategy(fastperiod=10, slowperiod=5)
    with pytest.raises(ValueError, match="signalperiod must be a positive integer"):
        MACDStrategy(signalperiod=0)
    with pytest.raises(ValueError, match="trend_period must be a positive integer"):
        MACDStrategy(trend_period=-1)
    logger.info("Correctly rejected invalid periods")

def test_missing_columns(macd_strategy: MACDStrategy):
    """Test signals with missing required columns."""
    invalid_data = pd.DataFrame({"open": [100, 101, 102]})
    with pytest.raises(ValueError, match="DataFrame missing required columns"):
        macd_strategy.calculate_signals(invalid_data)
    logger.info("Correctly rejected data with missing columns")

def test_position_tracking(macd_strategy: MACDStrategy, trend_data: pd.DataFrame):
    """Test position tracking affects signals."""
    macd_strategy.is_holding_position = False
    buy_signal, sell_signal = macd_strategy.calculate_signals(trend_data.iloc[:50])  # Rising
    assert buy_signal is True, "Should allow buy when not holding"
    assert sell_signal is False, "No sell when not holding"

    macd_strategy.update_position(True, False)
    buy_signal, sell_signal = macd_strategy.calculate_signals(trend_data.iloc[:50])  # Rising again
    assert buy_signal is False, "Should block buy when already holding"
    assert sell_signal is False, "No sell yet during rise"

    buy_signal, sell_signal = macd_strategy.calculate_signals(trend_data)  # Full trend
    assert buy_signal is False, "No buy when holding"
    assert sell_signal is True, "Should allow sell when holding during fall"
    logger.info("Position tracking correctly influenced signals")