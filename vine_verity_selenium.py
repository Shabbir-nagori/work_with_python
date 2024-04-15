#%%
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service as ChromeService
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.action_chains import ActionChains
import time

#%%
# def web_wait_until(selector, timeout=25, by=By.CSS_SELECTOR):
#     return WebDriverWait(driver, timeout).until(
#         EC.visibility_of_element_located((by, selector))
#     )

# print("Init done.")

#%%
#chrome_driver_binary = "D:/PYTHON/Pandas/SCRAPING/selenium/chromedriver-win64/chromedriver.exe"

options = webdriver.ChromeOptions()
#options.binary_location = "/Applications/Brave Browser.app/Contents/MacOS/Brave Browser"

# p = {'download.default_directory':'C:/Users/HP/Desktop'}
# options.add_experimental_option('prefs', p)
# options.add_argument("--headless")
# options.add_argument("--disable-gpu")
# options.add_argument("--window-size=2560,1440")

driver = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()), options=options)
print("Driver created.")

#%%

driver.maximize_window()
driver.get("https://www.oiv.int/index.php/what-we-do/viticulture-database-report?oiv=")
print("Loading page...")

#%%
driver.execute_script("window.scrollBy(0, 300);")
iframe_select = 'iframe[src^="https://app.powerbi.com/reportEmbed"]'
wait = WebDriverWait(driver, 20)  # Wait up to 20 seconds for the conditions to be met
iframe = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, iframe_select)))
#print("selecting frame in iframe....")
driver.switch_to.frame(iframe)
time.sleep(2)

# powerbi_iframe_selector = 'iframe[src^="https://app.powerbi.com/reportEmbed"]'
# iframe = web_wait_until(powerbi_iframe_selector)
# driver.execute_script("window.scrollTo(0, 300)")

# #%%
# print("Selecting Selectors...")
# driver.switch_to.frame(iframe)
#%%
#HME for Hover_Mouse_Element
HME = 'div.main-cell[role="gridcell"]'
HME_element = WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.CSS_SELECTOR, HME)))
#web_wait_until(HME)
action = webdriver.ActionChains(driver)
action.move_to_element(driver.find_element(By.CSS_SELECTOR, HME)).perform()
print("mouse hover complete...")
#time.sleep(5)

#%%
# TDS for the three-dot click
TDC = 'button.vcMenuBtn[data-testid="visual-more-options-btn"][aria-label="More options"]'
#web_wait_until(TDC)
drop_more_btn = driver.find_element(By.CSS_SELECTOR, TDC)
# Initialize ActionChains
action = ActionChains(driver)
# Move to the element and click on it
action.move_to_element(drop_more_btn).click().perform()
print("three dot click....")
#time.sleep(2)

#%%
#EBD for export data button
EDB = 'button.pbi-menu-item[data-testid="pbimenu-item.Export data"][title="Export data"]'
#web_wait_until(EDB)
EDB_element = WebDriverWait(driver, 20).until(EC.element_to_be_clickable((By.CSS_SELECTOR, EDB)))
EDB_clk = driver.find_element(By.CSS_SELECTOR, EDB)
action = ActionChains(driver)
# Move to the element and click on it
action.move_to_element(EDB_clk).click().perform()
print("Click on export data menu....")
time.sleep(3)

#%%
driver.execute_script("window.scrollBy(0, 300);")
# Export_data_clk_btn = 'button.mat-focus-indicator.pbi-modern-button.primaryBtn.exportButton.mat-button.mat-button-base[aria-label="Export"]'
# #Export_data_clk_btn = '/html/body/div[3]/div[2]/div/mat-dialog-container/div/div/export-data-dialog/mat-dialog-actions/button[1]'
# Export_data_clk = driver.find_element(By.XPATH, Export_data_clk_btn)
# action = ActionChains(driver)
# # Move to the element and click on it
# action.move_to_element(Export_data_clk).click().perform()
# time.sleep(2)
print("Downloading file...")
EDB = 'button.exportButton[aria-label="Export"]'
#web_wait_until(EDB)
WebDriverWait(driver, 20).until(EC.visibility_of_element_located((By.CSS_SELECTOR, EDB)))
driver.execute_script("""document.querySelector('"""+EDB+"""').click()""")
time.sleep(50)
print("Done!")

