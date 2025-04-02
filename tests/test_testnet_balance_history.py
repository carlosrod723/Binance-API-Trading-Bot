"""
Testnet Balance History API Tester

This script tests the get_account_snapshot Binance API method in isolation
to confirm whether it works correctly in the testnet environment.
"""

import os
import sys
import asyncio
import logging
from datetime import datetime, timedelta
from dotenv import load_dotenv

# Add the project root directory to the Python path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

# Now we can import from app
from app.utils.binance_utils import BinanceClient

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s'
)
logger = logging.getLogger("testnet_tester")

# Load environment variables
load_dotenv()

async def test_account_snapshot():
    """Test the get_account_snapshot method directly."""
    # Get testnet API credentials
    api_key = os.getenv("BINANCE_TESTNET_API_KEY")
    api_secret = os.getenv("BINANCE_TESTNET_API_SECRET")
    
    if not api_key or not api_secret:
        logger.error("Missing testnet API credentials")
        return
    
    # Initialize client
    client = BinanceClient(api_key, api_secret, testnet=True)
    try:
        await client.initialize()
        logger.info("Binance testnet client initialized successfully")
        
        # Calculate date range (last 30 days)
        end_time = datetime.now()
        start_time = end_time - timedelta(days=30)
        
        # Try to get account snapshots
        logger.info(f"Attempting to get account snapshots from {start_time.isoformat()} to {end_time.isoformat()}")
        
        try:
            snapshots = await client.client.get_account_snapshot(
                type='SPOT',
                startTime=int(start_time.timestamp() * 1000),
                endTime=int(end_time.timestamp() * 1000)
            )
            
            # Print response for inspection
            logger.info(f"API Response: {snapshots}")
            
            # Check if response contains expected data
            if isinstance(snapshots, dict):
                if 'code' in snapshots:
                    logger.info(f"Response code: {snapshots['code']}")
                
                if 'msg' in snapshots:
                    logger.info(f"Response message: {snapshots['msg']}")
                
                if 'snapshotVos' in snapshots:
                    snapshot_count = len(snapshots['snapshotVos'])
                    logger.info(f"Found {snapshot_count} snapshots")
                    
                    # Print first snapshot details if available
                    if snapshot_count > 0:
                        logger.info(f"First snapshot: {snapshots['snapshotVos'][0]}")
                else:
                    logger.warning("No 'snapshotVos' field found in response")
            else:
                logger.warning(f"Unexpected response type: {type(snapshots)}")
            
        except Exception as e:
            logger.error(f"Error calling get_account_snapshot: {str(e)}")
        
        # Also try other balance-related methods for comparison
        logger.info("Testing basic account info retrieval")
        account = await client.client.get_account()
        logger.info(f"Account info available: {bool(account)}")
        
        balances = await client.get_coin_balances()
        logger.info(f"Coin balances available: {len(balances) if balances else 0} coins")
        
    except Exception as e:
        logger.error(f"Test failed: {str(e)}")
    finally:
        # Clean up
        await client.close()
        logger.info("Test completed")

if __name__ == "__main__":
    asyncio.run(test_account_snapshot())