#google jobs scraper 
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from selenium.common.exceptions import TimeoutException
import time
import concurrent.futures
import os
import pandas as pd
from bs4 import BeautifulSoup
from collections import defaultdict
import re


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
            print("Clicked the 'jRKCUd' button")

        except Exception as e:
            print(f"'jRKCUd' button not found or could not be clicked: {e}")    

        # Scroll and load more jobs
        scroll_and_load_jobs(driver)

        # Locate the container of job listings
        jobs_table = driver.find_element(By.CLASS_NAME, "ZNyqGc")

        # Find each job element with class "EimVGf"
        each_jobs = jobs_table.find_elements(By.CLASS_NAME, "EimVGf")
        print(f"Client {client_id} - Found {len(each_jobs)} job elements.")

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
                                        print(f"Client {client_id} - Element timeout.")
                turn += 1
            except Exception as e:
                print(f"Client {client_id} - Error processing job: {e}")

    except Exception as e:
        print(f"Client {client_id} - Error: {e}")

    finally:
        # Close the WebDriver for this client
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


# Main function to run multiple clients concurrently
def main():
    client_queries = [
        "ai developer jobs AND housten"
        # ,"c++ jobs", "python developer jobs", "java software engineer jobs"
        # Add more queries for different clients here
    ]
    
    # Use ThreadPoolExecutor to run multiple clients in parallel
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        # Create a future for each client
        futures = [executor.submit(scrape_jobs_for_client, query, client_id + 1)
                   for client_id, query in enumerate(client_queries)]
        
        # Wait for all futures to complete
        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()
            except Exception as exc:
                print(f"Client generated an exception: {exc}")



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
                company = company.text.strip() if company else "Not Available"
                print(f"Company: {company}")

                # Extract job title
                job_div = soup.find('div', class_='JmvMcb')
                job_title = job_div.find('h1', class_='LZAQDf') if job_div else None
                job_title_text = job_title.text.strip() if job_title else "Not Available"
                print(f"Job Title: {job_title_text}")

                # Extract location
                location_div = job_div.find('div', class_='waQ7qe') if job_div else None
                location = location_div.text.strip() if location_div else "Not Available"
                # Remove text starting from "• via" and onward
                cleaned_location = re.sub(r'•\s*via.*', '', location).strip()
                print(f"Location: {cleaned_location}")

    
                # Extract job details (time posted, job type, salary)
                details = soup.find_all('span', class_='RcZtZb')
                        
                for job in details:                           
                    # Initialize variables
                    time_posted = "Not Available"
                    job_type = "Not Available"
                    salary = "Not Available"

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
                    
                    # Print the details for each job listing
                    print(f"Time Posted: {time_posted}")
                    print(f"Job Type: {job_type}")
                    print(f"Salary: {salary}")
                    print("---")  # Separator for readability
                

                # Extract job description
                job_description_div = soup.find('div', class_='NgUYpe')
                job_description = job_description_div.get_text(separator='\n').strip() if job_description_div else "No description available"
                # Define a regular expression pattern to match the unwanted text
                pattern = re.compile(r'\d+\s*more items(s)|More job highlights|Show full description|more item', re.IGNORECASE)

                # Replace unwanted text with an empty string
                cleaned_job_description = re.sub(pattern, '', job_description).strip()

                print(f"Job Description: {cleaned_job_description}")

                
                # Find the parent div
                parent_div = soup.find('div', class_='nNzjpf-cS4Vcb-PvZLI-wxkYzf')
                links=[]
                # Find all 'a' tags inside the parent div
                a_tags = parent_div.find_all('a')

                # Extract all the href links
                href_links = [a.get('href') for a in a_tags]
                
                # Print or process the links
                for link in href_links:
                    links.append(link)
                    print(links)


                # Append the extracted data to the dictionary for this client_id
                client_data[client_id]['company'].append(company)
                client_data[client_id]['job'].append(job_title_text)
                client_data[client_id]['location'].append(cleaned_location)
                client_data[client_id]['time_posted'].append(time_posted)
                client_data[client_id]['job_type'].append(job_type)
                client_data[client_id]['salary'].append(salary)
                client_data[client_id]['job_description'].append(job_description)
                client_data[client_id]['link'].append(", ".join(links))

        except UnicodeDecodeError as e:
            print(f"Error decoding file {file}: {e}")
        except Exception as e:
            print(f"An error occurred while processing file {file}: {e}")

    # Save the extracted data to CSV for each client
    for client_id, data in client_data.items():
        df = pd.DataFrame(data)
        df.to_csv(f"client_{client_id}_data.csv", index=False)
        print(f"Data for client {client_id} saved to client_{client_id}_data.csv")


    # Remove all HTML files after processing
    for file in os.listdir(data_directory):
        if file.endswith(".html"):
            try:
                os.remove(os.path.join(data_directory, file))
                print(f"Deleted file: {file}")
            except Exception as e:
                print(f"Error deleting file {file}: {e}")

if __name__ == "__main__":
    main()
    scraping_details("data")  # Specify your data directory