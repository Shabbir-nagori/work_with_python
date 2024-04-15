from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
import time

path = "D:/PYTHON/Pandas/SCRAPING/selenium/chrome-win64/chrome.exe"
s = Service(path)
driver=webdriver.Chrome(service=s)
driver.get("https://www.geeksforgeeks.org/")
