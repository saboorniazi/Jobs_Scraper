#google jobs scraper 
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from database import  get_all_jobs,store_client_job_data
from selenium.common.exceptions import TimeoutException
import time
import concurrent.futures
import os
import pandas as pd
from bs4 import BeautifulSoup
from collections import defaultdict
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from googleapiclient.discovery import build
import re
import schedule
import requests


# # Function to authenticate and connect to Google Sheets
# def connect_to_google_sheets(spreadsheet_name):
#     scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
#     creds = ServiceAccountCredentials.from_json_keyfile_name('credentials.json', scope)
#     client = gspread.authorize(creds)
#     sheet = client.open(spreadsheet_name).sheet1

#     # Make the sheet public
#     drive_service = build('drive', 'v3', credentials=creds)
#     file_id = client.open(spreadsheet_name).id
#     permission = {
#         'type': 'anyone',
#         'role': 'reader',
#     }
#     drive_service.permissions().create(fileId=file_id, body=permission).execute()

#     return sheet



# def append_data_to_sheet(sheet, data):
#     # Append data as rows
#     sheet.append_rows(data)


# Google Jobs Scraper


# Function to scrape jobs for a single client
def scrape_jobs_for_client(search_query, client_id):
    # Initialize the WebDriver for this client
    # options = webdriver.ChromeOptions()
    # options.add_argument('--headless')
    driver = webdriver.Chrome()
    
    try:
        # Open Google and search for the term
        driver.get("https://www.google.co.uk/")
        search_keyword = driver.find_element(By.ID, 'APjFqb')
        search_keyword.send_keys(search_query + Keys.ENTER)

        # Wait for the page to load and find the main container of job listings
        WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.CLASS_NAME, "ZNyqGc"))
        )

        #clicking on the more jobs button if availabe
        try:
            # Locate the container of job listings
            tabl_of_jobs = driver.find_element(By.CLASS_NAME, "ZNyqGc")
            # Find the div with jscontroller="dNBcld"
            div_with_jscontroller = tabl_of_jobs.find_element(By.XPATH, './/div[@jscontroller="dNBcld"]')
            # Find the div with jsname="aad1Bf"
            div_element = div_with_jscontroller.find_element(By.XPATH, './/div[@jsname="aad1Bf"]')
            # Wait up to 20 seconds for the next button to become clickable
            next_button = WebDriverWait(div_element, 20).until(
                EC.element_to_be_clickable((By.CLASS_NAME, 'jRKCUd'))
            )
            next_button.click()
            print("Google : Clicked the 'jRKCUd' button")

        except Exception as e:
            print(f"Google : 'jRKCUd' button not found or could not be clicked: {e}")    

        # Scroll and load more jobs
        scroll_and_load_jobs(driver)
        # Locate the container of job listings
        jobs_table = driver.find_element(By.CLASS_NAME, "ZNyqGc")
        # Find each job element with class "EimVGf"
        each_jobs = jobs_table.find_elements(By.CLASS_NAME, "EimVGf")
        print(f"Google : Client {client_id} - Found {len(each_jobs)} job elements.")

        # Loop through the job elements
        turn = 1
        for job in each_jobs:
            try:
                # Open a new file for each job/turn
                with open(f'data/client_{client_id}_job_{turn}.html', 'w', encoding='utf-8') as file:
                    job_contents = job.find_elements(By.CLASS_NAME, "L5NwLd")
                    
                    for job_content in job_contents:
                        mqj2af_elements = job_content.find_elements(By.CLASS_NAME, "mqj2af")
                        
                        for mqj2af_element in mqj2af_elements:
                            y1Aese_divs = mqj2af_element.find_elements(By.XPATH, './/div[@jsname="y1Aese"]')
                            
                            for y1Aese_div in y1Aese_divs:
                                a_tags = y1Aese_div.find_elements(By.CLASS_NAME, "MQUd2b")
                                
                                for a_tag in a_tags:
                                    try:
                                        ActionChains(driver).move_to_element(a_tag).click(a_tag).perform()                                        
                                        
                                        WebDriverWait(driver, 10).until(
                                            EC.presence_of_element_located((By.ID, 'Sva75c'))
                                        )
                                        job_details = WebDriverWait(driver, 10).until(
                                            EC.presence_of_element_located((By.XPATH, './/div[@jsname="CGzTgf"]'))
                                        )
                                        
                                        inner_html = job_details.get_attribute('innerHTML')
                                        file.write(inner_html + "<br>")
                                        
                                    except TimeoutException:
                                        print(f"Google : Client {client_id} - Element timeout.")
                turn += 1
            except Exception as e:
                print(f"Google : Client {client_id} - Error processing job: {e}")

    except Exception as e:
        print(f"Google : Client {client_id} - Error: {e}")

    finally:
        driver.quit()


# Function to scroll down the page and load more jobs
def scroll_and_load_jobs(driver):
    last_height = driver.execute_script("return document.body.scrollHeight")
    
    while True:
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(3)  # Adjust this based on network speed
        new_height = driver.execute_script("return document.body.scrollHeight")
        if new_height == last_height:
            break
        last_height = new_height

# Define patterns
time_posted_keywords = ['sec', 'seconds', 'hour', 'hours', 'day', 'days', 'year', 'years']
job_type_keywords = ['Part-time', 'Half-time', 'Full-time', 'Contractor']
salary_symbols = [
    '$', '€', '£', '¥', 'Fr', 'SFr', 'C$', 'A$', 'NZ$', '元', 'HK$', 'S$', '₩', '₹', 'R$', 
    'ر.س', 'د.إ', 'ر.ق', 'د.ك', '﷼', 'د.ب', '₦', 'KSh', '¢', '₱', '฿', '₫', 'Rs', '₨', '৳', 
    'S/', '₺', '₪'
]


def scraping_details(data_directory="data"):
    # Dictionary to hold data for each client
    client_data = defaultdict(lambda: {
        'company': [], 
        'job': [], 
        'location': [], 
        'time_posted': [], 
        'job_type': [], 
        'salary': [], 
        'job_description': [], 
        'link': []
    })

    # Regex to extract client_id from filename
    client_id_pattern = re.compile(r"client_(\d+)_job_\d+\.html")

    # Loop through each file in the specified directory
    for file in os.listdir(data_directory):
        try:
            # Match files with client_id in the filename
            match = client_id_pattern.match(file)
            if match:
                client_id = match.group(1)  # Extract client_id from filename

                # Open the file with UTF-8 encoding
                with open(f"{data_directory}/{file}", encoding='utf-8') as f:
                    html_doc = f.read()

                # Parse the HTML content using BeautifulSoup
                soup = BeautifulSoup(html_doc, 'html.parser')

                # Extract company name
                company = soup.find('div', class_='UxTHrf')
                company = company.text.strip() if company else " "
                # print(f"Company: {company}")

                # Extract job title
                job_div = soup.find('div', class_='JmvMcb')
                job_title = job_div.find('h1', class_='LZAQDf') if job_div else None
                job_title_text = job_title.text.strip() if job_title else " "
                # print(f"Job Title: {job_title_text}")

                # Extract location
                location_div = job_div.find('div', class_='waQ7qe') if job_div else None
                location = location_div.text.strip() if location_div else " "
                # Remove text starting from "• via" and onward
                cleaned_location = re.sub(r'•\s*via.*', '', location).strip()
                # print(f"Location: {cleaned_location}")
   
                # Extract job details (time posted, job type, salary)
                details = soup.find_all('span', class_='RcZtZb')
                        
                for job in details:                           
                    # Initialize variables
                    time_posted = ""
                    job_type = ""
                    salary = ""

                    if len(details) > 0:
                        text0 = details[0].text.strip()

                        if any(keyword in text0.lower() for keyword in time_posted_keywords):
                            if not any(symbol in text0 for symbol in salary_symbols):
                                time_posted = text0
                        # Check for job_type
                        if any(keyword in text0 for keyword in job_type_keywords):
                            job_type = text0
                        
                        # Check for salary
                        if any(symbol in text0 for symbol in salary_symbols):
                            salary = text0

                    if len(details) > 1:
                        text1 = details[1].text.strip()
                        # Check for time_posted
                        if any(keyword in text1.lower() for keyword in time_posted_keywords):
                            if not any(symbol in text1 for symbol in salary_symbols):
                                time_posted = text1
                        
                        # Check for job_type
                        if any(keyword in text1 for keyword in job_type_keywords):
                            job_type = text1
                        
                        # Check for salary
                        if any(symbol in text1 for symbol in salary_symbols):
                            salary = text1

                    if len(details) > 2:
                        text2 = details[2].text.strip()
                        # Check for time_posted
                        if any(keyword in text2.lower() for keyword in time_posted_keywords):
                            if not any(symbol in text2 for symbol in salary_symbols):
                                time_posted = text2
                        
                        # Check for job_type
                        if any(keyword in text2 for keyword in job_type_keywords):
                            job_type = text2
                        
                        # Check for salary
                        if any(symbol in text2 for symbol in salary_symbols):
                            salary = text2
                    
                    # # Print the details for each job listing
                    # print(f"Time Posted: {time_posted}")
                    # print(f"Job Type: {job_type}")
                    # print(f"Salary: {salary}")
                    # print("---")  # Separator for readability
                

                # Extract job description
                job_description_div = soup.find('div', class_='NgUYpe')
                job_description = job_description_div.get_text(separator='\n').strip() if job_description_div else "No description available"
                # Define a regular expression pattern to match the unwanted text
                pattern = re.compile(r'\d+\s*more items(s)|More job highlights|Show full description|more item', re.IGNORECASE)
                # Replace unwanted text with an empty string
                cleaned_job_description = re.sub(pattern, '', job_description).strip()
                # print(f"Job Description: {cleaned_job_description}")
                
                # Finding the all available links
                parent_div = soup.find('div', class_='nNzjpf-cS4Vcb-PvZLI-wxkYzf')
                links=[]
                # Find all 'a' tags inside the parent div
                a_tags = parent_div.find_all('a')
                # Extract all the href links
                href_links = [a.get('href') for a in a_tags]              
                # Print or process the links
                for link in href_links:
                    links.append(link)
                    # print(links)


                # Append the extracted data to the dictionary for this client_id
                client_data[client_id]['company'].append(company)
                client_data[client_id]['job'].append(job_title_text)
                client_data[client_id]['location'].append(cleaned_location)
                client_data[client_id]['time_posted'].append(time_posted)
                client_data[client_id]['job_type'].append(job_type)
                client_data[client_id]['salary'].append(salary)
                client_data[client_id]['job_description'].append(cleaned_job_description)
                client_data[client_id]['link'].append(", ".join(links))

        except UnicodeDecodeError as e:
            print(f"Google : Error decoding file {file}: {e}")
        except Exception as e:
            print(f"Google : An error occurred while processing file {file}: {e}")

    # Save the extracted data to CSV for each client
    for client_id, data in client_data.items():
        # Create a DataFrame with the fetched data
        df = pd.DataFrame(data)       
        # Define the CSV filename specific to the client
        file_path = f"client_{client_id}_data.csv"        
        # Check if the CSV file already exists
        if os.path.exists(file_path):
            # Load the existing CSV data into a DataFrame
            existing_df = pd.read_csv(file_path)            
            # Concatenate the existing data with the new data
            combined_df = pd.concat([existing_df, df], ignore_index=True)            
            # Drop duplicate rows based on all columns
            combined_df = combined_df.drop_duplicates()            
            # Save the combined data back to the CSV file without writing the header again
            combined_df.to_csv(file_path, index=False)
            print(f"Google : Jobs Data for client {client_id} appended to {file_path} with duplicates skipped.")
        else:
            # Create a new CSV file with the DataFrame
            df.to_csv(file_path, index=False)
            print(f"Google : Jobs Data for client {client_id} saved to {file_path}")


        # spreadsheet_name = f"Client_{client_id}_Data"
        # sheet = connect_to_google_sheets(spreadsheet_name)
        # append_data_to_sheet(sheet, [df.columns.values.tolist()] + df.values.tolist())
        # print(f"Data for client {client_id} saved to Google Sheet: https://docs.google.com/spreadsheets/d/{sheet.spreadsheet.id}/edit")

        # # Store data in SQLite database
        # for i in range(len(data['company'])):
        #     job_data = {
        #         'company': data['company'][i],
        #         'job': data['job'][i],
        #         'location': data['location'][i],
        #         'time_posted': data['time_posted'][i],
        #         'job_type': data['job_type'][i],
        #         'salary': data['salary'][i],
        #         'job_description': data['job_description'][i],
        #         'link': data['link'][i],
        #     }
        #     store_client_job_data(client_id, job_data)

    # Remove all HTML files after processing
    for file in os.listdir(data_directory):
        if file.endswith(".html"):
            try:
                os.remove(os.path.join(data_directory, file))
                print(f"Google : Deleted file: {file}")
            except Exception as e:
                print(f"Google : Error deleting file {file}: {e}")

# linkeedin jobs Scraper

api_key = '66e1a139876b3e2df95fa2b7'
url = "https://api.scrapingdog.com/linkedinjobs/"
page = "2"
# geoid ="100293800"

# Function to retrieve job IDs
def linkedin_jobs_urls(api_key: str, field: str, geoid: str, page: str):
    params = {
        "api_key": api_key,
        "field": field,
        "geoid": geoid,
        "page": page
    }

    response = requests.get(url, params=params)

    if response.status_code == 200:
        data = response.json()
        job_ids = [job['job_id'] for job in data]  # Assuming the response contains 'job_id'
        return job_ids
    else:
        print(f"Linkedin : Request failed with status code: {response.status_code}")
        return []


def append_jobs_for_client(client_id: str, field: str, geoid: str, page: str):
    job_ids = linkedin_jobs_urls(api_key, field, geoid, page)

    if not job_ids:
        print(f"Linkedin : No jobs found for client {client_id}.")
        return None  # No jobs found, return None
    
    # Print the number of jobs found
    print(f"Linkedin : Found {len(job_ids)} jobs for client {client_id}.")

    # Initialize lists for job details
    company = []
    job = []
    location = []
    time_posted = []
    job_type_list = []
    salary = []
    job_description = []
    link = []
    
    # Fetch job details for each job ID
    for job_id in job_ids:
        params = {
            "api_key": api_key,
            "job_id": job_id
        }
        response = requests.get(url, params=params)

        if response.status_code == 200:
            job_data = response.json()
            # Check if 'job_apply_link' exists
            job_apply_link = job_data[0].get('job_apply_link', 'N/A')
            
            # Append job details to respective lists
            company.append(job_data[0]['company_name'])
            job.append(job_data[0]['job_position'])
            location.append(job_data[0]['job_location'])
            time_posted.append(job_data[0]['job_posting_time'])
            job_type_list.append(job_data[0]['Employment_type'])
            salary.append(job_data[0].get('base_pay', 'N/A'))  # If 'base_pay' is missing, use 'N/A'
            job_description.append(job_data[0]['job_description'])
            link.append(job_apply_link)
            
        else:
            print(f"Failed to retrieve details for job ID {job_id}: Status code {response.status_code}")

    # Check if there are any job positions
    if job:

        df = pd.DataFrame({
        'company': company,
        'job': job,
        'location': location,
        'time_posted': time_posted,
        'job_type': job_type_list,
        'salary': salary,
        'job_description': job_description,
        'link': link,
        
    })
    
    # Define the CSV filename specific to the client
    file_path = f"client_{client_id}_data.csv"
    # Check if the CSV file already exists
    if os.path.exists(file_path):
        # Load the existing CSV data into a DataFrame
        existing_df = pd.read_csv(file_path)        
        # Concatenate the existing data with the new data
        combined_df = pd.concat([existing_df, df], ignore_index=True)        
        # Drop duplicate rows based on all columns
        combined_df = combined_df.drop_duplicates()        
        # Save the combined data back to the CSV file without writing the header again
        combined_df.to_csv(file_path, index=False)
        print(f"Linkedin Data for client {client_id} appended to {file_path} with duplicates skipped.")
    else:
        # Create a new CSV file with the DataFrame
        df.to_csv(file_path, index=False)
        print(f"Linkedin Data for client {client_id} saved to {file_path}")



    # spreadsheet_name = f"Client_{client_id}_Data"
    # sheet = connect_to_google_sheets(spreadsheet_name)
    # append_data_to_sheet(sheet, [df.columns.values.tolist()] + df.values.tolist())
    # print(f"Data for client {client_id} saved to Google Sheet: https://docs.google.com/spreadsheets/d/{sheet.spreadsheet.id}/edit")

    # # Store data in SQLite database
    # for i in range(len(data['company'])):
    #     job_data = {
    #         'company': data['company'][i],
    #         'job': data['job'][i],
    #         'location': data['location'][i],
    #         'time_posted': data['time_posted'][i],
    #         'job_type': data['job_type'][i],
    #         'salary': data['salary'][i],
    #         'job_description': data['job_description'][i],
    #         'link': data['link'][i],
    #     }
    #     store_client_job_data(client_id, job_data)


def main():
    # List of client queries, each representing a unique search string
    linkedin_client_queries = [
        ("1", ["java developer","Amazone"], "2988507"),    # a client with multiple job queries at the same location
        ("2", ["python developer"], "2988507")  
        # Add more queries and client_ids as needed
    ]
    
    google_client_queries = [
        (["java developer jobs Multan","Amazone jobs Multan"], "1"),  # a client with multiple job queries
        (["python developer jobs Multan"], "2")
        # Add more queries and client_ids as needed 
    ]


    # Use ThreadPoolExecutor to run multiple tasks in parallel
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        futures = []

        # Submit tasks for append_jobs_for_client (LinkedIn)
        for client_id, queries, geoid in linkedin_client_queries:
            for query in queries:
                futures.append(
                    executor.submit(append_jobs_for_client, client_id, query, geoid, page)
                )
        
        # Submit tasks for scrape_jobs_for_client (Google)
        for queries, client_id in google_client_queries:
            for query in queries:
                futures.append(
                    executor.submit(scrape_jobs_for_client, query, client_id)
                )
        
        # Wait for all futures to complete
        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()  # Ensure any exceptions within a future are raised
            except Exception as exc:
                print(f"Main : Client generated an exception: {exc}")




def scheduled_scraping_details():

    # Schedule scraping_details("data") to run every 5 minutes
    schedule.every(3).minutes.do(scraping_details, "data")
    # Schedule main() to run every 5 minutes
    schedule.every(3).minutes.do(main)

   

    while True:
        schedule.run_pending()
        time.sleep(1)



if __name__ == "__main__":
    main()
    scraping_details("data") # Specify your data directory
    scheduled_scraping_details()

    
