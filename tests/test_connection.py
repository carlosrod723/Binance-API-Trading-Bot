import pytest
import os
from dotenv import load_dotenv
import logging
from typing import AsyncGenerator
from app.utils.binance_utils import BinanceClient

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Mark all tests as async
pytestmark = pytest.mark.asyncio

@pytest.fixture(scope="module")
async def binance_test_client() -> AsyncGenerator[BinanceClient, None]:
    """Provide an async Binance Testnet client for testing connectivity."""
    load_dotenv()
    api_key = os.getenv("BINANCE_TESTNET_API_KEY")
    api_secret = os.getenv("BINANCE_TESTNET_API_SECRET")
    if not api_key or not api_secret:
        pytest.skip("Skipping connection tests: BINANCE_TESTNET_API_KEY or BINANCE_TESTNET_API_SECRET not set")
    
    client = BinanceClient(api_key, api_secret, testnet=True)
    try:
        await client.initialize()
        logger.info(f"Initialized client with API Key: {api_key[:4]}... (masked)")
        yield client
    except Exception as e:
        pytest.fail(f"Failed to initialize Binance AsyncClient: {str(e)}")
    finally:
        try:
            await client.close()
            logger.info("Binance test client closed")
        except Exception as e:
            logger.error(f"Error closing Binance test client: {e}")

async def test_basic_connectivity(binance_test_client: BinanceClient):
    """Test basic connectivity to Binance Testnet."""
    try:
        account_info = await binance_test_client.client.get_account()
        assert account_info is not None, "Should retrieve account info"
        assert "balances" in account_info, "Account info should include balances"
        logger.info("Successfully connected to Binance Testnet")
    except Exception as e:
        logger.error(f"Connectivity test failed: {e}")
        pytest.fail(f"Failed to connect to Binance Testnet: {e}")

async def test_connectivity_invalid_keys():
    """Test connectivity with invalid API keys."""
    client = BinanceClient("invalid_key", "invalid_secret", testnet=True)
    try:
        await client.initialize()
        pytest.fail("Should raise an exception with invalid keys")
    except Exception as e:
        logger.info(f"Expected failure with invalid keys: {e}")
        assert "authentication" in str(e).lower() or "api" in str(e).lower(), "Should fail due to authentication error"
    finally:
        if client.client:
            await client.close()

async def test_connectivity_missing_keys():
    """Test client creation with missing keys."""
    with pytest.raises(ValueError):
        client = BinanceClient("", "", testnet=True)
        await client.initialize()  # Should raise before this point, but included for completeness
    logger.info("Correctly rejected missing keys during initialization")