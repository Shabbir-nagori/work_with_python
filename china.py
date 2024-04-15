#%%
import os
import time
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service as ChromeService
import undetected_chromedriver as uc
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from datetime import datetime
from dateutil import rrule
import urllib.parse
import pandas as pd
from math import ceil
import pg8000
import json
import calendar


def web_wait_until(selector, timeout=25, by=By.CSS_SELECTOR):
    return WebDriverWait(driver, timeout).until(
        EC.visibility_of_element_located((by, selector))
    )
print("Starting...")
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
#browser_executable_path = "/home/forge/.cache/puppeteer/chrome/linux-1069273/chrome-linux/chrome"
#browser_executable_path = "/Applications/Brave Browser.app/Contents/MacOS/Brave Browser"
#browser_executable_path = "/Users/harish/.cache/selenium/chrome/mac-arm64/122.0.6261.128/Google Chrome for Testing.app"
browser_executable_path = "C:\Program Files (x86)\Google\Chrome\Application\chrome.exe"

#browser_version_main = 109
browser_version_main = 123

headless = False

source_name = 'china-stats-customs'
directory = 'china-stats-customs'
projectRoot = os.path.abspath(os.path.join(os.getcwd(), '.'))
downloadPath = os.path.join(projectRoot, directory)
if not os.path.exists(downloadPath):
    os.makedirs(downloadPath)

options = uc.ChromeOptions()
p = {'download.default_directory':downloadPath, 'download.prompt_for_download': False}
options.add_experimental_option('prefs', p)
options.add_argument("--disable-gpu")
options.add_argument("--window-size=2560,1440")

driver  = uc.Chrome(browser_executable_path=browser_executable_path, version_main=browser_version_main, headless=headless, options=options)

## change UA header to avoid detection
ua = driver.execute_script("return navigator.userAgent")
ua = ua.replace('HeadlessChrome/', 'Chrome/')
driver.execute_cdp_cmd('Network.setUserAgentOverride', {'userAgent': ua})
driver.maximize_window()
print("Driver created...")


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

#getting metadata fields
driver.get('http://stats.customs.gov.cn/queryDataForEN/queryDataByWhereEn')
web_wait_until('form#Search_form')
#time.sleep(0.5)
time.sleep(2)

params = {}
for mp in ['codeLength', 'currentStartTime', 'currentEndTime', 'currentDateBySource']:
    params[mp] = driver.find_element(By.CSS_SELECTOR, f'input[name="{mp}"]').get_attribute('value')
    try:
        params[mp] = int(params[mp])
    except:
        pass

print('Got meta fields...')
time.sleep(1)

#%%
base_url = 'http://stats.customs.gov.cn/queryDataForEN/queryDataListEn'
def getUrlParams(month, iEType):
    prm = params.copy()
    selectStartTime = month
    selectEndTime = month
    if selectStartTime>=prm['currentStartTime']:
        if prm['currentEndTime']>=selectEndTime:
            prm['selectTableState'] = '1'
    elif prm['currentStartTime']>selectEndTime:
        prm['selectTableState'] = '2'
    else:
        prm['selectTableState'] = '3'

    prm['currencyType'] = 'usd'
    prm['year'] = dt.strftime('%Y')
    prm['startMonth'] = int(dt.strftime('%m'))
    prm['endMonth'] = int(dt.strftime('%m'))

    prm['outerField1'] = 'CODE_TS'
    prm['outerValue1'] = ''
    prm['outerField2'] = 'ORIGIN_COUNTRY'
    prm['outerValue2'] = ''
    prm['outerField3'] = 'TRADE_MODE'
    prm['outerValue3'] = ''
    prm['outerField4'] = 'TRADE_CO_PORT'
    prm['outerValue4'] = ''

    prm['pageNum'] = '1'
    prm['pageSize'] = '2000'
    prm['orderType'] = 'CODE ASC DEFAULT'

    prm['iEType'] = iEType

    return prm

def getUrl(month, iEType):
    prm = getUrlParams(month, iEType)
    return base_url+'?'+urllib.parse.urlencode(prm)
#%%
## update entry based on last updated date
last_updated_date = datetime(int(str(params['currentEndTime'])[:4]), int(str(params['currentEndTime'])[4:]), 1)
end_date = last_updated_date
start_date = datetime(int(str(params['currentEndTime'])[:4])-2, int(str(params['currentEndTime'])[4:]), 1)

for dt in rrule.rrule(rrule.MONTHLY, dtstart=start_date, until=end_date):
    for tradeTypeStr in ['import', 'export']:
        db_year = int(dt.strftime('%Y'))
        db_month = int(dt.strftime('%m'))
        filename = str(dt.strftime('%Y%m'))+'-'+tradeTypeStr+'.csv'
        file_path = os.path.join(str(db_year), filename)
        check_query = """
            SELECT COUNT(*) FROM incremental_process_data_log
            WHERE path = %s AND last_updated_at = %s;
            """
        cursor.execute(check_query, (file_path, last_updated_date))
        result = cursor.fetchone()
        if result[0]==0:
            cursor.execute("INSERT INTO public.incremental_process_data_log (source, year, month, path, type, last_updated_at, updated_at) VALUES (%s, %s, %s, %s, 'text/csv', %s, NOW()) RETURNING id", (source_name, db_year, db_month, file_path, last_updated_date))
            #cursor.execute("INSERT INTO public.incremental_process_data_log (source, year, month, path, type, last_updated_at, updated_at) VALUES (%s, %s, %s, %s, 'text/csv', %s, NOW()) RETURNING id", (source_name, db_year, db_month, os.path.join(str(db_year), filename), last_updated_date))
            log_id = cursor.fetchone()[0]
            url = getUrl(int(dt.strftime('%Y%m')), '1' if tradeTypeStr=='import' else '0')
            cursor.execute("INSERT INTO public.incremental_process_data_log_events (log_id, event_name, output_path, updated_at) VALUES (%s, 'UPDATE', %s, NOW())", (log_id, json.dumps({"link": url, "filename": filename})))

#%%
params['pageSize'] = 2000
list_to_download = getForLastStatus(connection, 'UPDATE')
print("Total files to downloaded: ", len(list_to_download))

for item in list_to_download:
    d_item = json.loads(item['output_path'])
    filename = d_item['filename']  # The desired filename
    YearofFile = filename[:4]

    totalSize = 1
    totalPages = ceil(totalSize/int(params['pageSize']))
    finalDf = None

    pageNum = 1

    url = d_item['link']
    driver.get(url)
    web_wait_until('#table tbody tr', timeout=320) # pls define timeout as per your internet speed
    time.sleep(1)

    prev_df = None
    #while pageNum<=totalPages:
    while pageNum<=2:
        ## start downloading

        totalSize = int(driver.find_element(By.CSS_SELECTOR, f'input[name="totalSize"]').get_attribute('value'))
        if totalSize == 0:
            break
        totalPages = ceil(totalSize/int(params['pageSize']))
        

        htmlTable = driver.find_element(By.CSS_SELECTOR, "#table").get_attribute('outerHTML')
        df = pd.read_html(htmlTable, converters={'Commodity code':str})[0]
        if prev_df is not None and df.equals(prev_df):
            raise Exception("Something went wrong could be network issue..")


        if finalDf is None:
            finalDf = df
        else:
            finalDf = pd.concat([finalDf, df])


        
        print('Finished page:', pageNum, 'of', totalPages, 'for', filename)
        pageNum += 1

        if pageNum>totalPages:
            break

        prev_df = df
        ##got next page click
        driver.execute_script("document.querySelector('#pageSize').value = '"+str(params['pageSize'])+"'")
        driver.execute_script("document.querySelector('#hidPageSize').value = '"+str(params['pageSize'])+"'")
        time.sleep(2)
        driver.execute_script("doSearch("+str(pageNum)+")")
        #web_wait_until('#table tbody tr', timeout=120)
        web_wait_until('#table tbody tr', timeout=320)  # pls define timeout as per your internet speed

        time.sleep(2)


    year_folder = os.path.join(downloadPath, YearofFile)  
    if not os.path.exists(year_folder):
        os.makedirs(year_folder)

    o_filepath = os.path.join(year_folder, filename)
    

    finalDf.to_csv(o_filepath, index=False)

    #getting relative path
    o_filepath = os.path.join(*o_filepath.split(os.sep)[-3:])

    cursor.execute("""INSERT INTO incremental_process_data_log_events (log_id, event_name, output_path, updated_at)
    VALUES (%s, 'DOWNLOAD', %s, CURRENT_TIMESTAMP)
    """, (item['id'], o_filepath))

    print(f"{filename} is completely Downloaded...")

# %%
print('All files downloaded... Located at ', downloadPath)
driver.quit()
# %%