import requests
import json

# Test connection to the Binance API
try:
    url = "http://localhost:8000/api/v1/test/connection"
    headers = {
        "Content-Type": "application/json",
        "Origin": "http://localhost"
    }
    data = {
        "apiKey": "testkey123",
        "apiSecret": "testsecret123",
        "isTestnet": True
    }
    
    # First send an OPTIONS request to test CORS preflight
    options_response = requests.options(url, headers={
        "Origin": "http://localhost",
        "Access-Control-Request-Method": "POST"
    })
    
    print(f"OPTIONS {url}")
    print(f"Status Code: {options_response.status_code}")
    print("Headers:")
    for key, value in options_response.headers.items():
        if key.startswith("Access-Control"):
            print(f"  {key}: {value}")
    
    # Now send the actual POST request
    response = requests.post(url, headers=headers, data=json.dumps(data))
    
    print(f"\nPOST {url}")
    print(f"Status Code: {response.status_code}")
    print("Headers:")
    for key, value in response.headers.items():
        if key.startswith("Access-Control"):
            print(f"  {key}: {value}")
    
    print("\nResponse JSON:")
    try:
        json_response = response.json()
        print(json.dumps(json_response, indent=2))
    except:
        print("Could not parse JSON response")
        print("Raw response:", response.text)
except Exception as e:
    print(f"Error: {e}")
