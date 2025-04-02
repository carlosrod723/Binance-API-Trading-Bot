import requests

def test_cors(url):
    # Test OPTIONS request for CORS headers
    options_response = requests.options(url, headers={
        "Origin": "http://localhost", 
        "Access-Control-Request-Method": "GET"
    })
    
    print(f"OPTIONS {url}")
    print(f"Status Code: {options_response.status_code}")
    print("Headers:")
    for key, value in options_response.headers.items():
        if key.startswith("Access-Control"):
            print(f"  {key}: {value}")
    
    # Test actual GET request with Origin header
    get_response = requests.get(url, headers={"Origin": "http://localhost"})
    
    print(f"\nGET {url}")
    print(f"Status Code: {get_response.status_code}")
    print("Headers:")
    for key, value in get_response.headers.items():
        if key.startswith("Access-Control"):
            print(f"  {key}: {value}")

# Test the health endpoint
test_cors("http://localhost:8000/health")

# Test the initialization status endpoint
test_cors("http://localhost:8000/api/v1/initialization/status")
