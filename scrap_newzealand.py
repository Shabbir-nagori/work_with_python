# %%
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from bs4 import BeautifulSoup
import pg8000
import requests
import datetime
import zipfile
import time
import json
import re
import os

#%%
# Establish a database connection
connection = pg8000.connect(user='postgres',
                      host='localhost',
                      database='db_test',
                      port=5432,
                      password='nagori123')
connection.autocommit=True

cursor = connection.cursor()


#%%
# Main variables
url = "https://www.stats.govt.nz/large-datasets/csv-files-for-download/overseas-merchandise-trade-datasets"
source_name = "New_Zealand"
directory = "New_Zealand"

#%%
# Get the content from the URL
path = "D:\PYTHON\Pandas\SCRAPING\selenium\chromedriver-win64\chromedriver.exe"
s = Service(path)
driver=webdriver.Chrome(service=s)
driver.get(url)
time.sleep(5)
# Parse the content with BeautifulSoup
soup = BeautifulSoup(driver.page_source, 'html5lib')
driver.quit()


#%%
links = []
li = soup.select('#main > section > div > div > div > article > div > div:first-of-type > article > div > div > div > ul:nth-of-type(2) > li')
for item in li:
    a_tag = item.find('a')
    href = a_tag.get('href')    
    filename = href.split('/')[-1]
    match = re.match(r'(\d{4})_', filename)
    year_of_data = match.group(1)
    # Remove .zip extension from the filename
    #filename = filename.replace('.zip', '')
    date_string = item.get_text().strip()    
    # Use regular expression to extract date components (adjust pattern if needed)
    date_match = re.search(r"(\d{1,2}) ([A-Za-z]+) (\d{4})", date_string)

    if date_match:
        day, month_name, year = date_match.groups()

        # Convert month name to number
        month = {"January": 1, "February": 2, "March": 3, "April": 4, "May": 5, "June": 6,
                        "July": 7, "August": 8, "September": 9, "October": 10, "November": 11, "December": 12}[month_name]

        # Convert day, month, and year to integers
        day = int(day)
        month = int(month)
        year = int(year)

        # Format the date as YYYY-MM-DD
        last_updated_date = f"{year:04d}-{month:02d}-{day:02d}"
        #print(f"last_updated_date: {formatted_date}")
    else:
        print("No date found in the expected format.")
    links.append((href, filename, last_updated_date, year_of_data))
    #print(f'HREF: {href}, FILENAME: {filename}, last_updated_date: {last_updated_date}, YEAR: {year_of_data}')


#%%
# Function to ensure the download directory exists
def ensure_dir(directory_path):
    if not os.path.exists(directory_path):
        os.makedirs(directory_path)
# def ensure_dir(file_path):
#     directory = os.path.dirname(file_path)
#     if not os.path.exists(directory):
#         os.makedirs(directory)


#%%
# Function to download a file from a given URL
def download_file(url, directory, filename, year, retries=5, delay=1):
    # local_filename = url.split('/')[-1]
    # local_filename = sanitize_filename(local_filename)
    directory = os.path.join(directory, year)
    ensure_dir(directory)  # Ensure the directory exists
    path = os.path.join(directory, filename)
    
    for attempt in range(retries):
        try:
            headers = {'User-Agent': 'Mozilla/5.0'}
            with requests.get(url, stream=True, headers=headers, verify=False) as r:
                r.raise_for_status()
                with open(path, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        f.write(chunk)
            return directory
        except requests.exceptions.RequestException as e:
            print(f"Download failed: {e}, attempt {attempt + 1} of {retries}. Retrying in {delay} seconds...")
            time.sleep(delay)
            delay *= 2  # Exponential back-off
    
    print(f"Failed to download {url} after {retries} attempts.")
    return None


#%%
# Function to extract .zip files and then remove the .zip files
def extract_and_cleanup(directory, year):
    directory = os.path.join(directory, year)
    for filename in os.listdir(directory):
        if filename.endswith('.zip'):
            file_path = os.path.join(directory, filename)
            # Extract the .zip file
            try:
                with zipfile.ZipFile(file_path, 'r') as zip_ref:
                    zip_ref.extractall(directory)
                print(f"Extracted: {filename}")
            except zipfile.BadZipFile as e:
                print(f"Failed to extract {filename}: {e}")
                continue
            # Remove the .zip file
            os.remove(file_path)
            print(f"Removed: {filename}")



#%%
def query(conn, query, args=()):
    c = conn.cursor()
    c.execute(query, args)
    rows = c.fetchall()
    keys = [k[0] for k in c.description]
    results = [dict(zip(keys, row)) for row in rows]
    return results


def getForLastStatus(conn, last_status):
    c = conn.cursor()
    return query(conn, """SELECT c.id,
       c.source,
       c.year,
       c.month,
       c.path,
       c.type,
       c.last_updated_at,
       c.created_at,
       c.updated_at,

       p1.id as event_id,
       p1.event_name,
       p1.output_path,
       p1.created_at as event_created_at,
       p1.updated_at as event_updated_at
    FROM incremental_process_data_log c
    JOIN incremental_process_data_log_events p1 ON (c.id = p1.log_id)
    LEFT OUTER JOIN incremental_process_data_log_events p2 ON ( c.id = p2.log_id AND
        (p1.id < p2.id) )
    WHERE p2.id IS NULL AND c.source=%s AND p1.event_name=%s""", (source_name, last_status))



# %%
# Initialize a set to track downloaded filenames
downloaded_files = set()
now_date = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
for href, filename, last_updated_date, year_of_data in links:
    if href.endswith('.zip'):
        # Check if the file has already been processed
        if filename not in downloaded_files:
            #updated_date = datetime.datetime.strptime(date, "%d/%m/%Y %H:%M:%S")
            check_query = """
            SELECT COUNT(*) FROM incremental_process_data_log
            WHERE path = %s AND last_updated_at = %s;
            """
            cursor.execute(check_query, (filename, last_updated_date))
            result = cursor.fetchone()
            if result[0]==0:
                cursor.execute("INSERT INTO public.incremental_process_data_log (source, year, path, type, last_updated_at, updated_at) VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP) RETURNING id", (source_name, year_of_data, filename, 'text/csv', last_updated_date))
                log_id = cursor.fetchone()[0]
                #print(f"Source: New_Zealand, Year: {year_of_data}, path: {filename}, type: text/csv, last_updated_at: {last_updated_date}, Created_at: {now_date}, updated_at: {now_date}")
                cursor.execute("INSERT INTO public.incremental_process_data_log_events (log_id, event_name, output_path, updated_at) VALUES (%s, 'UPDATE', %s, CURRENT_TIMESTAMP)", (log_id, json.dumps({"link": href, "filename": filename})))
                print("file update successfully....", filename)
            

# %%
list_to_download = getForLastStatus(connection, 'UPDATE')
print("Total files to download: ", len(list_to_download))
is_error = False
# Ensure the directory(Folder) exists
ensure_dir(directory)
for item in list_to_download:
    #try:
    d_item = json.loads(item['output_path'])
    link = d_item['link']  # 'link' contains the full URL to download
    filename = d_item['filename']  # The desired filename
    year_of_data = str(item['year'])
    
    # Download the file
    print(f"Downloading {filename}...")

    downloaded_filename = download_file(link, directory, filename, year_of_data)
    #downloaded_filename = downloaded_filename.replace(".zip",".csv")
    if downloaded_filename:
        print(f"Downloaded and saved as {downloaded_filename}")
        print(filename, "file downloaded.")
        print("Starting extraction and cleanup...")
        extract_and_cleanup(directory, year_of_data)

        print("Extraction and cleanup completed.")
        cursor.execute("""
            INSERT INTO incremental_process_data_log_events (log_id, event_name, output_path, updated_at) 
            VALUES (%s, 'DOWNLOAD', %s, CURRENT_TIMESTAMP)
        """, (item['id'], downloaded_filename))
        
        # Perform additional actions as necessary, e.g., extracting files, processing data
    
# Remember to commit changes if auto-commit is not enabled
connection.commit()
