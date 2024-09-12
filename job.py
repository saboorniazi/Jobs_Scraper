import requests
import json
# Base URL for the Google Jobs API
base_url = "https://google-jobs.p.rapidapi.com/"

# Query parameters for the initial request
query_params = {
    "keyword": "python developer",
    "location": "Pakistan",
    "offset": "0",
    "posted": "all"
}

# Headers for the API request
headers = {
    "x-rapidapi-key": "YOUR_API_KEY",
    "x-rapidapi-host": "google-jobs.p.rapidapi.com"
}

# Make the initial request to get job offers
response = requests.get(base_url, headers=headers, params=query_params)
response_data = response.json()

# Extract job offers
offers = response_data.get('offers', [])

# Print or save the offers
print("Job offers:")
for offer in offers:
    print(offer)
    # Save offers to a file for later processing
    with open('job_offers.json', 'w') as file:
        json.dump(offers, file, indent=4)
