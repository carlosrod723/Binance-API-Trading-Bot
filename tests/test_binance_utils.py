import pytest
import pytest_asyncio
import os
from dotenv import load_dotenv
import asyncio
from binance.exceptions import BinanceAPIException, BinanceOrderException
from app.utils.binance_utils import BinanceClient
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

pytestmark = pytest.mark.asyncio

@pytest_asyncio.fixture(scope="module")
async def binance_test_client():
    """Provide a Binance Testnet client for testing."""
    load_dotenv()
    api_key = os.getenv("BINANCE_TESTNET_API_KEY")
    api_secret = os.getenv("BINANCE_TESTNET_API_SECRET")
    if not api_key or not api_secret:
        pytest.skip("Skipping Binance tests: BINANCE_TESTNET_API_KEY or API_SECRET not set")
    
    client = BinanceClient(api_key, api_secret, testnet=True)
    try:
        await client.initialize()
        yield client
    except Exception as e:
        pytest.fail(f"Failed to initialize Binance client: {str(e)}")
    finally:
        await client.close()

async def clear_open_orders(client: BinanceClient, symbol: str = "BTCUSDT") -> None:
    """Cancel all open orders for the given symbol asynchronously."""
    try:
        open_orders = await client.get_open_positions()
        for order in open_orders:
            if order["coinPair"] == symbol:
                await client.client.cancel_order(symbol=symbol, orderId=order["orderId"])
                logger.info(f"Cancelled order {order['orderId']} for {symbol}")
    except (BinanceAPIException, BinanceOrderException) as e:
        logger.warning(f"Error clearing orders for {symbol}: {e}")

async def test_binance_client_init_valid_keys(binance_test_client: BinanceClient):
    """Test client initialization with valid keys."""
    assert binance_test_client is not None, "Client should be created with valid keys"
    assert binance_test_client.client is not None, "Internal AsyncClient should be initialized"

async def test_binance_client_init_invalid_keys():
    """Test client initialization with invalid keys."""
    client = BinanceClient("invalid_key", "invalid_secret", testnet=True)
    with pytest.raises(ValueError):
        await client.initialize()

async def test_get_account_balance(binance_test_client: BinanceClient):
    """Test retrieving the overall balance."""
    balance = await binance_test_client.get_account_balance()
    assert balance is not None or balance == 0, "Should return a balance (float or None)"
    assert isinstance(balance, float) or balance is None, "Balance should be a float or None"
    if balance is not None:
        assert balance >= 0, "Balance should be non-negative"

async def test_get_coin_balances(binance_test_client: BinanceClient):
    """Test retrieving individual coin balances."""
    balances = await binance_test_client.get_coin_balances()
    assert balances is not None, "Should return a list of balances (possibly empty)"
    assert isinstance(balances, list), "Balances should be a list"

async def test_create_buy_order(binance_test_client: BinanceClient):
    """Test creating a buy order (Testnet)."""
    await clear_open_orders(binance_test_client, "BTCUSDT")
    order_id = await binance_test_client.create_buy_order("BTCUSDT", 0.001, 50000.0)
    assert order_id is not None, "Buy order should return an order ID"
    assert isinstance(order_id, str), "Order ID should be a string"
    await binance_test_client.client.cancel_order(symbol="BTCUSDT", orderId=order_id)

async def test_create_buy_order_with_stoploss_takeprofit(binance_test_client: BinanceClient):
    """Test stop loss and take profit for a buy order."""
    await clear_open_orders(binance_test_client, "BTCUSDT")
    order_id = await binance_test_client.create_buy_order("BTCUSDT", 0.001, 50000.0, stop_loss=45000.0, take_profit=55000.0)
    assert order_id is not None, "Buy order should return an order ID"
    assert isinstance(order_id, str), "Order ID should be a string"
    await binance_test_client.client.cancel_order(symbol="BTCUSDT", orderId=order_id)

async def test_create_sell_order(binance_test_client: BinanceClient):
    """Test creating a sell order (Testnet)."""
    await clear_open_orders(binance_test_client, "BTCUSDT")
    order_id = await binance_test_client.create_sell_order("BTCUSDT", 0.001, 50000.0)
    assert order_id is not None, "Sell order should return an order ID"
    assert isinstance(order_id, str), "Order ID should be a string"
    await binance_test_client.client.cancel_order(symbol="BTCUSDT", orderId=order_id)

async def test_create_sell_order_with_stoploss_takeprofit(binance_test_client: BinanceClient):
    """Test stop loss and take profit for a sell order."""
    await clear_open_orders(binance_test_client, "BTCUSDT")
    order_id = await binance_test_client.create_sell_order("BTCUSDT", 0.001, 50000.0, stop_loss=55000.0, take_profit=45000.0)
    assert order_id is not None, "Sell order should return an order ID"
    assert isinstance(order_id, str), "Order ID should be a string"
    await binance_test_client.client.cancel_order(symbol="BTCUSDT", orderId=order_id)

async def test_exit_position(binance_test_client: BinanceClient):
    """Test exiting a position (Testnet)."""
    await clear_open_orders(binance_test_client, "BTCUSDT")
    result = await binance_test_client.exit_position("BTCUSDT")
    assert isinstance(result, bool), "exit_position should return a boolean"
    assert result is True, "Should return True if no position or successful exit"

async def test_get_klines(binance_test_client: BinanceClient):
    """Test fetching klines data."""
    klines = await binance_test_client.get_klines("BTCUSDT")
    assert klines is not None, "get_klines should return data"
    assert isinstance(klines, list), "klines should be a list"
    assert len(klines) > 0, "klines should not be empty"

async def test_get_open_positions(binance_test_client: BinanceClient):
    """Test retrieving open positions."""
    await clear_open_orders(binance_test_client, "BTCUSDT")
    order_id = await binance_test_client.create_buy_order("BTCUSDT", 0.001, 50000.0)
    positions = await binance_test_client.get_open_positions()
    assert isinstance(positions, list), "Open positions should be a list"
    assert any(p["orderId"] == order_id for p in positions), "Should include the test order"
    await binance_test_client.client.cancel_order(symbol="BTCUSDT", orderId=order_id)

async def test_get_closed_positions(binance_test_client: BinanceClient):
    """Test retrieving closed positions."""
    positions = await binance_test_client.get_closed_positions()
    assert isinstance(positions, list), "Closed positions should be a list"

async def test_create_buy_order_invalid_symbol(binance_test_client: BinanceClient):
    """Test buy order with an invalid symbol."""
    order_id = await binance_test_client.create_buy_order("INVALIDUSDT", 0.001, 50000.0)
    assert order_id is None, "Buy order with invalid symbol should return None"

async def test_create_buy_order_insufficient_funds(binance_test_client: BinanceClient):
    """Test buy order with insufficient funds (very high size)."""
    order_id = await binance_test_client.create_buy_order("BTCUSDT", 1000000.0, 50000.0)
    assert order_id is None, "Buy order with insufficient funds should return None"