# simplest_test.py
from binance.client import Client
from binance.exceptions import BinanceAPIException
import os
from dotenv import load_dotenv

load_dotenv()

# Get keys from .env
api_key = os.getenv("BINANCE_TESTNET_API_KEY")
api_secret = os.getenv("BINANCE_TESTNET_API_SECRET")

# Print the keys for VERIFICATION
print(f"API Key (from .env): {api_key}")
print(f"API Secret (from .env): {api_secret}")

try:
    # Create the client instance *inside* the try block
    client = Client(api_key, api_secret)
    print("Client created successfully.")  # If we get here, client creation worked

    # Simplest possible API call: get server time
    server_time = client.get_server_time()
    print(f"Binance Server Time: {server_time}")
    print("SUCCESS: Connected to Binance Testnet and retrieved server time.")

except BinanceAPIException as e:
    print(f"Binance API Error: {e}")
except Exception as e:
    print(f"General Error: {e}")