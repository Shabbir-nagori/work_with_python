# # %%
# from bs4 import BeautifulSoup
# import pg8000
# import requests
# from datetime import datetime
# import re
# import os
# import json
# import zipfile
# import time
# import shutil
# import subprocess
# from dotenv import dotenv_values
# import argparse
# #%%
# # Argument parsing setup
# parser = argparse.ArgumentParser(description='Script arguments')
# parser.add_argument('--year', type=int, help='Year of trade to specifically process')
# parser.add_argument('--dont_trigger_s3', type=bool, default=False, help='S3 Trigger Flag')

# cli_args = parser.parse_args()

# #%%
# # Load environment variables
# env_var = dotenv_values('.env')

# # Establish a database connection
# connection = pg8000.connect(
#     user=env_var['DB_USERNAME_CSTD'],
#     password=env_var['DB_PASSWORD_CSTD'],
#     host=env_var['DB_HOST_CSTD'],
#     port=env_var['DB_PORT_CSTD'],
#     database=env_var['DB_DATABASE_CSTD']
# )
# connection.autocommit = True
# cursor = connection.cursor()

# print("Starting....")
# # Main variables
# source_name = "colombia-dane-gov"
# directory_name = "colombia-dane-gov"
# projectRoot = os.path.abspath(os.path.join(os.getcwd(), '.'))
# directory = os.path.join(projectRoot, directory_name)
# urls = {
#     "Export": "https://microdatos.dane.gov.co/index.php/catalog/472/get-microdata",
#     "Import": "https://microdatos.dane.gov.co/index.php/catalog/473/get-microdata"
# }
# headers = {
#     'User-Agent': 'Mozilla/5.0'
# }

# #%%
# # Function to ensure the download directory exists
# def ensure_dir(directory_path):
#     if not os.path.exists(directory_path):
#         os.makedirs(directory_path)

# # Function to download a file from a given URL
# def download_file(url, directory, filename, year, tradetype, retries=5, delay=1):
#     directory = os.path.join(directory, year, tradetype)
#     ensure_dir(directory)  # Ensure the directory exists
#     path = os.path.join(directory, filename)
    
#     for attempt in range(retries):
#         try:
#             headers = {'User-Agent': 'Mozilla/5.0'}
#             with requests.get(url, stream=True, headers=headers, verify=False) as r:
#                 r.raise_for_status()
#                 with open(path, 'wb') as f:
#                     for chunk in r.iter_content(chunk_size=8192):
#                         f.write(chunk)
#             return directory
#         except requests.exceptions.RequestException as e:
#             print(f"Download failed: {e}, attempt {attempt + 1} of {retries}. Retrying in {delay} seconds...")
#             time.sleep(delay)
#             delay *= 2  # Exponential back-off
    
#     print(f"Failed to download {url} after {retries} attempts.")
#     return None

# def extract_and_cleanup(directory, year, tradetype):
#     directory = os.path.join(directory, year, tradetype)
#     for filename in os.listdir(directory):
#         file_path = os.path.join(directory, filename)
#         if filename.endswith('.zip'):
#             try:
#                 extract_zip_recursively(file_path, directory)
#                 print(f"Extracted: {filename}")
#             except zipfile.BadZipFile as e:
#                 print(f"Failed to extract {filename}: {e}")

# #%%
# def move_files_to_target_and_cleanup(source_directory, target_directory):
#     """
#     Moves files from source_directory to target_directory and removes any empty directories.
#     """
#     for root, dirs, files in os.walk(source_directory, topdown=False):
#         for name in files:
#             file_path = os.path.join(root, name)
#             shutil.move(file_path, os.path.join(target_directory, name))

#         # Attempt to remove the directory, will only succeed if empty
#         if root != target_directory:  # Prevent trying to delete the target directory itself
#             try:
#                 os.rmdir(root)
#             except OSError as e:
#                 print(f"Directory not empty: {e}")

# def extract_zip_recursively(zip_path, target_directory, processed_files=None):
#     if processed_files is None:
#         processed_files = set()
#     if zip_path in processed_files:
#         return
#     processed_files.add(zip_path)
#     if not os.path.exists(zip_path):
#         print(f"File not found: {zip_path}")
#         return
    
#     # Extract directly to the target_directory
#     with zipfile.ZipFile(zip_path, 'r') as zip_ref:
#         zip_ref.extractall(target_directory)
    
#     os.remove(zip_path)  # Remove the .zip file after extraction

#     # Iterate through the directory to find any nested zip files and extract them
#     for root, dirs, files in os.walk(target_directory):
#         for filename in files:
#             if filename.endswith('.zip'):
#                 file_path = os.path.join(root, filename)
#                 extract_zip_recursively(file_path, target_directory, processed_files)
#             elif filename.endswith('.dta') or filename.endswith('.sav') or filename.endswith('.txt'):
#                 try:
#                     os.remove(os.path.join(root, filename))
#                     print(f"Removed: {filename}")
#                 except OSError as e:
#                     print(f"Error: {e.strerror} - {filename}")
    
#     # After all extractions, move files up if needed and clean up empty directories
#     move_files_to_target_and_cleanup(target_directory, target_directory)

# #%%
# def query(conn, query, args=()):
#     c = conn.cursor()
#     c.execute(query, args)
#     rows = c.fetchall()
#     keys = [k[0] for k in c.description]
#     results = [dict(zip(keys, row)) for row in rows]
#     return results

# def getForLastStatus(conn, last_status):
#     c = conn.cursor()
#     return query(conn, """SELECT c.id,
#        c.source,
#        c.year,
#        c.month,
#        c.path,
#        c.type,
#        c.last_updated_at,
#        c.created_at,
#        c.updated_at,
#        p1.id as event_id,
#        p1.event_name,
#        p1.output_path,
#        p1.created_at as event_created_at,
#        p1.updated_at as event_updated_at
#     FROM incremental_process_data_log c
#     JOIN incremental_process_data_log_events p1 ON (c.id = p1.log_id)
#     LEFT OUTER JOIN incremental_process_data_log_events p2 ON ( c.id = p2.log_id AND
#         (p1.id < p2.id) )
#     WHERE p2.id IS NULL AND c.source=%s AND p1.event_name=%s""", (source_name, last_status))

# def is_within_last_three_years(year, last_updated_year):
#     return last_updated_year - 2 <= year <= last_updated_year
# # %%
# # Process each URL
# for tradetype, url in urls.items():
#     response = requests.get(url, headers=headers)
#     response.raise_for_status()  # Check that the request was successful

#     # Parse the content with BeautifulSoup
#     soup = BeautifulSoup(response.content, 'html.parser')

#     links = []

#     # get last_updated_date
#     span_tag = soup.select('body > div.container-fluid-n > div > div.page-body-full.study-metadata-page > div.container-fluid.page-header > div > div > div.col-md-10 > div.dataset-footer-bar.mt-2 > span:nth-child(2) > small > strong')
#     date_text = span_tag[0].get_text().strip()
#     date_obj = datetime.strptime(date_text, '%B %d, %Y')
#     Last_updated_date = date_obj.strftime('%Y-%m-%d')
#     Last_updated_year = int(date_obj.strftime('%Y'))
#     # find download link in div tag
#     divs = soup.select('#tabs-1 > div > div > div > div[data-file-type="microdata"] > div.resource-left-col > div')
#     for div in divs:
#         input_tag = div.find('input')
#         filename = input_tag['title']
#         onclick_attr = input_tag['onclick']
#         url_match = re.search(r"https?://[^\s,']+", onclick_attr)
#         href = url_match.group(0) if url_match else None

#         # To extract the year from the filename
#         year_match = re.search(r'\d{4}', filename)
#         year = int(year_match.group(0)) if year_match else None
#         if cli_args.year:
#             should_process = year == cli_args.year
#         else:
#             should_process = is_within_last_three_years(year, Last_updated_year)

#         if should_process:                
#             # Append details as a dictionary to the list
#             links.append((href, filename, Last_updated_date, year))
#             print("Added for download:", filename)
# #%%

#     # Initialize a set to track downloaded filenames
    
#     for href, filename, Last_updated_date, year in links:
#         check_query = """
#         SELECT COUNT(*) FROM incremental_process_data_log
#         WHERE path = %s AND last_updated_at = %s;
#         """
#         cursor.execute(check_query, (filename, Last_updated_date))
#         result = cursor.fetchone()
#         if result[0] == 0:
#             cursor.execute("INSERT INTO public.incremental_process_data_log (source, year, path, type, last_updated_at, updated_at) VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP) RETURNING id", (source_name, year, filename, 'text/csv', Last_updated_date))
#             log_id = cursor.fetchone()[0]
#             cursor.execute("INSERT INTO public.incremental_process_data_log_events (log_id, event_name, output_path, updated_at) VALUES (%s, 'UPDATE', %s, CURRENT_TIMESTAMP)", (log_id, json.dumps({"link": href, "filename": filename})))
#             print("File update successfully....", filename)

# #%%
# list_to_download = getForLastStatus(connection, 'UPDATE')
# print("Total files to download: ", len(list_to_download))
# ensure_dir(directory)

# for item in list_to_download:
#     d_item = json.loads(item['output_path'])
#     link = d_item['link']
#     filename = d_item['filename']
#     year_of_data = str(item['year'])

#     # Determine tradetype based on filename or item's attributes
#     tradetype = "Export" if "expo" in filename.lower() else "Import"

#     print(f"Downloading {filename}...")

#     downloaded_filename = download_file(link, directory, filename, year_of_data, tradetype)
#     if downloaded_filename:
#         print(f"Downloaded and saved as {downloaded_filename}")
#         print(filename, "file downloaded.")

#         print("Starting extraction and cleanup...")
#         extract_and_cleanup(directory, year_of_data, tradetype)

#         print("Extraction and cleanup completed.")
#         #only_filepath = os.path.join(downloaded_filename, filename)
#         cursor.execute("""
#             INSERT INTO incremental_process_data_log_events (log_id, event_name, output_path, updated_at) 
#             VALUES (%s, 'DOWNLOAD', %s, CURRENT_TIMESTAMP)
#         """, (item['id'], downloaded_filename))

# # %%
# if cli_args.dont_trigger_s3 is False:
#     subprocess.run(["php", "artisan", "trigger:s3-uploader-job", source_name], cwd=projectRoot)
# print("Done...!")


#%%
import argparse

# Example setup for argparse
parser = argparse.ArgumentParser()
parser.add_argument('--year', type=int, help="Specify the year for which data is needed")
cli_args = parser.parse_args()

# Let's say we have this variable defined as the most recent year for which data is updated.
Last_updated_year = 2022

# Adjusting the code for the requirement
if cli_args.year and cli_args.year <= Last_updated_year:
    selected_year = cli_args.year
else:
    selected_year = Last_updated_year + 2

# Now selected_year will have the correct year based on the user's input or the default requirement.
print("Selected Year:", selected_year)