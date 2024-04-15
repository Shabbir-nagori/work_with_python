# %%
import sys
import os
import json
import time
import re
import requests
import subprocess
import argparse
from bs4 import BeautifulSoup
from datetime import datetime
from dotenv import dotenv_values
import pg8000

# Argument parsing setup
parser = argparse.ArgumentParser(description='Script arguments')
parser.add_argument('--year', type=int, help='Year of trade to specifically process')
parser.add_argument('--dont_trigger_s3', type=bool, default=False, help='S3 Trigger Flag')

cli_args = parser.parse_args()

# Load environment variables
env_var = dotenv_values('.env')

# Establish a database connection
connection = pg8000.connect(
    user=env_var['DB_USERNAME_CSTD'],
    password=env_var['DB_PASSWORD_CSTD'],
    host=env_var['DB_HOST_CSTD'],
    port=env_var['DB_PORT_CSTD'],
    database=env_var['DB_DATABASE_CSTD']
)
connection.autocommit = True
cursor = connection.cursor()

print("Starting....")

# %%
# Main variables
source_name = "thailand"
directory_name = "thailand"
projectRoot = os.path.abspath(os.path.join(os.getcwd(), '.'))
directory = os.path.join(projectRoot, directory_name)
urls = {
    "Export": "https://catalog.customs.go.th/dataset/ctm_06_12",
    "Import": "https://catalog.customs.go.th/dataset/ctm_06_11"
}
headers = {
    'User-Agent': 'Mozilla/5.0'
}

# %%
# Helper functions
def ensure_dir(directory_path):
    if not os.path.exists(directory_path):
        os.makedirs(directory_path)

def thai_date_to_english(filename):
    match = re.search(r'(\d{2})\.csv$', filename)
    thai_year_suffix = match.group(1)
    last_updated_year = 2565 - 65  # Adjust base year as per your data range
    full_thai_year = last_updated_year + int(thai_year_suffix)
    gregorian_year = full_thai_year - 543
    return gregorian_year

def download_file(url, directory, year, tradetype, filename, retries=5, delay=1):
    directory = os.path.join(directory, str(year), tradetype)
    ensure_dir(directory)
    path = os.path.join(directory, filename)
    for attempt in range(retries):
        try:
            with requests.get(url, stream=True, headers=headers, verify=False) as r:
                r.raise_for_status()
                with open(path, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        f.write(chunk)
            return directory
        except requests.exceptions.RequestException as e:
            print(f"Download failed: {e}, attempt {attempt + 1} of {retries}. Retrying in {delay} seconds...")
            time.sleep(delay)
            delay *= 2
    print(f"Failed to download {url} after {retries} attempts.")
    return None


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


def is_within_last_three_years(year, last_updated_year):
    return last_updated_year - 2 <= year <= last_updated_year
#%%
# Process each URL
for tradetype, url in urls.items():
    response = requests.get(url, headers=headers)
    response.raise_for_status()  # Successful request
    soup = BeautifulSoup(response.content, 'html.parser')
    # get last_updated_date
    font_tag = soup.select('#content > div > article > div > section:nth-child(6) > table > tbody > tr:nth-child(35) > td')
    date_text = font_tag[0].get_text().strip()
    # Mapping of Thai months to English
    thai_to_english_months = {
        'มกราคม': 'January',
        'กุมภาพันธ์': 'February',
        'มีนาคม': 'March',
        'เมษายน': 'April',
        'พฤษภาคม': 'May',
        'มิถุนายน': 'June',
        'กรกฎาคม': 'July',
        'สิงหาคม': 'August',
        'กันยายน': 'September',
        'ตุลาคม': 'October',
        'พฤศจิกายน': 'November',
        'ธันวาคม': 'December'
    }
    # Split the Thai date text
    day, thai_month, thai_year = date_text.split()

    # Convert Thai month to English month
    english_month = thai_to_english_months[thai_month]

    # Convert year from Buddhist calendar to Gregorian calendar
    gregorian_year = int(thai_year) - 543

    # Reform the date string in English
    english_date_text = f"{day} {english_month} {gregorian_year}"

    # Parse the English date string
    date_obj = datetime.strptime(english_date_text, '%d %B %Y')

    # Format the date as needed
    last_updated_date = date_obj.strftime('%Y-%m-%d')
    Last_updated_year = int(date_obj.strftime('%Y'))
    # get downloading link 
    div_tag = soup.select('#dataset-resources > ul > li.resource-item > div.dropdown.btn-group')
    csv_links = []

    for div in div_tag:
        ul_tags = div.find_all('ul')
        for ul in ul_tags:
            li_tags = ul.find_all('li')
            if len(li_tags) >= 2:
                second_li = li_tags[1]
                a_tag = second_li.find('a')
                link = a_tag['href']
                if link.endswith('.csv'):
                    filename = link.split('/')[-1]
                    year = thai_date_to_english(filename)
                    if cli_args.year:
                        if cli_args.year > Last_updated_year:
                            should_process = year == Last_updated_year
                        else:
                            should_process = year == cli_args.year
                    else:
                        should_process = is_within_last_three_years(year, Last_updated_year)

                    if should_process:
                        csv_links.append(link)
                        print("Added for download:", filename," for :", year)

    for link in csv_links:
        filename = link.split('/')[-1]
        year = thai_date_to_english(filename)        
        check_query = """
        SELECT COUNT(*) FROM incremental_process_data_log
        WHERE path = %s AND last_updated_at = %s;
        """
        cursor.execute(check_query, (filename, last_updated_date))
        result = cursor.fetchone()
        if result[0] == 0:
            cursor.execute("INSERT INTO public.incremental_process_data_log (source, year, path, type, last_updated_at, updated_at) VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP) RETURNING id", (source_name, year, filename, 'text/csv', last_updated_date))
            log_id = cursor.fetchone()[0]
            cursor.execute("INSERT INTO public.incremental_process_data_log_events (log_id, event_name, output_path, updated_at) VALUES (%s, 'UPDATE', %s, CURRENT_TIMESTAMP)", (log_id, json.dumps({"link": link, "filename": filename})))
            print("File update successfully....", filename)

#%%
list_to_download = getForLastStatus(connection, 'UPDATE')
print("Total files to download: ", len(list_to_download))
ensure_dir(directory)

for item in list_to_download:
    d_item = json.loads(item['output_path'])
    link = d_item['link']
    filename = d_item['filename']
    year = str(item['year'])

    # Determine tradetype based on filename or item's attributes
    tradetype = "Export" if "expo" in filename.lower() else "Import"

    # Download the file
    print(f"Downloading {filename} for year {year}")

    downloaded_filename = download_file(link, directory, year, tradetype, filename)
    if downloaded_filename:
        print(f"Downloaded and saved as {downloaded_filename}")
        print(filename, "file downloaded.")
        only_filepath = os.path.join(downloaded_filename, filename)
        cursor.execute("""
            INSERT INTO incremental_process_data_log_events (log_id, event_name, output_path, updated_at) 
            VALUES (%s, 'DOWNLOAD', %s, CURRENT_TIMESTAMP)
        """, (item['id'], only_filepath))

# %%
if cli_args.dont_trigger_s3 is False:
    subprocess.run(["php", "artisan", "trigger:s3-uploader-job", source_name], cwd=projectRoot)
print("Done...!")
