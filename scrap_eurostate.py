# %%
import os
import requests
from bs4 import BeautifulSoup
import time
import subprocess
import datetime
import pg8000
import re
import json

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
url = "https://ec.europa.eu/eurostat/api/dissemination/files/?sort=-3&dir=comext%2FCOMEXT_DATA%2FPRODUCTS"
directory = "Eurostat_files"
#file_prefixes = "full202401"
file_prefixes = [f"full{year}{str(month).zfill(2)}" for year in [2023, 2024] for month in range(1, 13)]
source_name = "eurostat"

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


#%%
# Function to ensure the download directory exists
def ensure_dir(dir_name):
    if not os.path.exists(dir_name):
        os.makedirs(dir_name)



#%%
# Function to remove special character in url
def sanitize_filename(filename):
    #Remove or replace characters in a string that are not allowed in filenames.
    invalid_chars = '<>:"/\\|?*'
    for char in invalid_chars:
        filename = filename.replace(char, '_')
    return filename



#%%
# Function to download a file from a given URL
def download_file(url, directory, filename, retries=5, delay=1):
    # local_filename = url.split('/')[-1]
    # local_filename = sanitize_filename(local_filename)
    path = os.path.join(directory, filename)
    
    for attempt in range(retries):
        try:
            headers = {'User-Agent': 'Mozilla/5.0'}
            with requests.get(url, stream=True, headers=headers) as r:
                r.raise_for_status()
                with open(path, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        f.write(chunk)
            return path
        except requests.exceptions.RequestException as e:
            print(f"Download failed: {e}, attempt {attempt + 1} of {retries}. Retrying in {delay} seconds...")
            time.sleep(delay)
            delay *= 2  # Exponential back-off
    
    print(f"Failed to download {url} after {retries} attempts.")
    return None



#%%
# Function to extract .7z files, rename .dat files to .csv, and then remove the .7z files
ZIPEXE_PATH = r"C:\Program Files\7-Zip\7z.exe"
def extract_and_cleanup(directory):
    for filename in os.listdir(directory):
        if filename.endswith('.7z'):
            file_path = os.path.join(directory, filename)
            # Extract the .7z file
            try:
                subprocess.run([ZIPEXE_PATH, 'x', file_path, f'-o{directory}'], check=True)
                print(f"Extracted: {filename}")
            except subprocess.CalledProcessError as e:
                print(f"Failed to extract {filename}: {e}")
                continue
            # Remove the .7z file
            os.remove(file_path)
            print(f"Removed: {filename}")
    
    for filename in os.listdir(directory):
        if filename.endswith('.dat'):
            base_name = os.path.splitext(filename)[0]
            new_name = f"{base_name}.csv"
            os.rename(os.path.join(directory, filename), os.path.join(directory, new_name))
            print(f"Renamed: {filename} to {new_name}")


#%%
# Ensure the directory(Folder) exists
ensure_dir(directory)

# Get the content from the URL  
response = requests.get(url)
response.raise_for_status()

# Parse the content with BeautifulSoup
soup = BeautifulSoup(response.content, 'html.parser')

# # Find all 'a' tags, as these are links
# links = soup.find_all('a', href=True)

# Initialize an empty list to collect 'href' attributes
links = []

# Find all 'tr' tags
for tr in soup.find_all('tr'):
    # Initialize variables to hold the extracted name and date
    name = None
    date = None
    month = None
    year = None
    # For each 'tr', findall the 'td'
    tds = tr.find_all('td')
    if tds:
        # In the found 'td', look for an 'a' tag
        a_tag = tds[0].find('a', href=True)
        if a_tag:
            name = a_tag.text.strip()
            date = tds[3].text.strip()
            href = a_tag['href']
            # Use regex to extract year and month
            match = re.search(r'full(\d{4})(\d{2})', href)
            if match:
                year = int(match.group(1))
                month = int(match.group(2))
            links.append((href, name, date, year, month))


#%%

# Initialize a set to track downloaded filenames
downloaded_files = set()
link_filename_pair = []
# Filter and download files that match the criteria
for href, name, date, year, month in links:
    #href = href.get('href')
    #if href.endswith('.7z') and file_prefixes in href:
    if any(href.endswith('.7z') and prefix in href for prefix in file_prefixes):
        full_url = f"https://ec.europa.eu{href}"
        filename = href.split('/')[-1]
        link_filename_pair.append({"link": full_url, "filename": name})
        # Check if the file has already been processed
        if filename not in downloaded_files:
            #updated_date = datetime.datetime.strptime(date, "%d/%m/%Y %H:%M:%S")
            check_query = """
            SELECT COUNT(*) FROM incremental_process_data_log
            WHERE path = %s AND last_updated_at = %s;
            """
            cursor.execute(check_query, (name, date))
            result = cursor.fetchone()
            if result[0]==0:
                downloaded_date = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                cursor.execute("INSERT INTO public.incremental_process_data_log (source, year, month, path, type, last_updated_at, updated_at) VALUES (%s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP) RETURNING id", ('eurostat', year, month, name, 'text/csv', date))
                log_id = cursor.fetchone()[0]
                print(f"Source: Eurostat, Year: {year}, Month: {month}, path: {name}, type: text/csv, last_updated_at: {date}, Created_at: {downloaded_date}, updated_at: {downloaded_date}")
                cursor.execute("INSERT INTO public.incremental_process_data_log_events (log_id, event_name, output_path, updated_at) VALUES (%s, 'UPDATE', %s, CURRENT_TIMESTAMP)", (log_id, json.dumps({"link": full_url, "filename": name})))
                print("file update successfully....", name)

#%%
list_to_download = getForLastStatus(connection, 'UPDATE')
print("Total files to download: ", len(list_to_download))
is_error = False
for item in list_to_download:
    #try:
    d_item = json.loads(item['output_path'])
    link = d_item['link']  # 'link' contains the full URL to download
    filename = d_item['filename']  # The desired filename
    
    # Download the file
    print(f"Downloading {filename}...")
    
    downloaded_filename = download_file(link, directory, filename)
    downloaded_filename = downloaded_filename.replace(".7z",".csv")
    if downloaded_filename:
        print(f"Downloaded and saved as {downloaded_filename}")
        print(filename, "files downloaded. Starting extraction and cleanup...")
        extract_and_cleanup(directory)

        print("Extraction and cleanup completed.")
        cursor.execute("""
            INSERT INTO incremental_process_data_log_events (log_id, event_name, output_path, updated_at) 
            VALUES (%s, 'DOWNLOAD', %s, CURRENT_TIMESTAMP)
        """, (item['id'], downloaded_filename))
        
        # Perform additional actions as necessary, e.g., extracting files, processing data
        
    else:
        print(f"Failed to download {filename}")
    # except Exception as e:
    #     print(f"Error processing {item}: {e}")

# Remember to commit changes if auto-commit is not enabled
connection.commit()

#%%
# # After downloading all files
# print("All files downloaded. Starting extraction and cleanup...")
# extract_and_cleanup(directory)

# print("Extraction and cleanup completed.")
# %%
