import requests
import json

# Test connection with isTestnet=True explicitly set
try:
    url = "http://localhost:8000/api/v1/test/connection"
    headers = {
        "Content-Type": "application/json",
        "Origin": "http://localhost"
    }
    data = {
        "apiKey": "RCG71TrVHEcE2gnHZM9PHIdhhQp9an52HCOuNC7rbwW2P2WT6rS1zwblJnMvPz89",
        "apiSecret": "v7nDS66CJXAQGJwPwNIdyTNFOyCvi5Mxl1hBNXTX8wh1FbQaltm76OCVfvYJqaD0",
        "isTestnet": True  # <-- Explicitly True
    }
    
    # Send the POST request
    response = requests.post(url, headers=headers, data=json.dumps(data))
    
    print(f"POST {url} with isTestnet=True")
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
