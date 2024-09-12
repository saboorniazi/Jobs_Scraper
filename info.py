import requests
import time

# Define the URL to check
url = "https://www.google.com/search?ibp=htl%3Bjobs&q=python+developer+United+Kingdom&htidocid=pLivUG02pBP3nJKbAAAAAA%3D%3D&hl=en-US&shndl=-1&shem=vslcca&source=sh%2Fx%2Fim%2Ftextlists%2Fdetail%2Fm1%2F1&mysharpfpstate=tldetail&htivrt=jobs&htiq=python+developer+United+Kingdom&htidocid=pLivUG02pBP3nJKbAAAAAA%3D%3D"

# Headers for the request
headers = {
    "x-rapidapi-key": "7ec565c85emsh59ddba37f4500f5p174642jsn1f4f36edc2e5",
    "x-rapidapi-host": "google-jobs.p.rapidapi.com"
}

# Retry logic parameters
max_retries = 5
retry_delay = 1

# Start retry loop
attempt = 0
while attempt < max_retries:
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()  # Check if the request was successful

        if response.text:
            print(response.json())
        else:
            print(f"Empty response received for URL: {url}")

        break  # Exit the retry loop on successful request
        
    except requests.exceptions.RequestException as e:
        attempt += 1
        print(f"Attempt {attempt}: An error occurred while fetching the job details: {e}")
        if attempt < max_retries:
            print(f"Retrying in {retry_delay} seconds...")
            time.sleep(retry_delay)
            retry_delay *= 2  # Exponential backoff
        else:
            print(f"Max retries reached for URL: {url}")

