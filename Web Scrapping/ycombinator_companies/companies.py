from selenium import webdriver
from selenium.webdriver.common.by import By
import time
import pandas as pd

driver = webdriver.Chrome()
driver.get("https://www.ycombinator.com/companies")
time.sleep(5)

list_companies = []
domain_name = []

  
driver.find_element(By.XPATH,"//a[@class = 'wFMmHIyWKCYOnrsVb3Yq']").click()
checkbox = driver.find_elements(By.CLASS_NAME,'iFFzDB_kifA_KgWdCfms')
# print(checkbox[46].text)
for ch in checkbox[8:47]:
    ch.click()
    time.sleep(1)
    while True:
        page_height = driver.execute_script("return document.body.scrollHeight")
        driver.execute_script("window.scrollTo(0,document.body.scrollHeight)")
        time.sleep(1)
        new_height = driver.execute_script("return document.body.scrollHeight")

        if page_height == new_height:
            break

    companies = driver.find_elements(By.XPATH,"//*[@class = 'CBY8yVfV0he1Zbv9Zwjx']")
    for i in range(len(companies)):
        list_companies.append(companies[i].text)

    hyperlinks = driver.find_elements(By.XPATH,"//a[@class = 'WxyYeI15LZ5U_DOM0z8F']")
    for i in range(len(hyperlinks)):
        hyperlinks[i].click()
        domain = driver.find_element(By.XPATH,"//div[@class = 'inline-block group-hover:underline']").text
        domain_name.append(domain)
        print(domain)
        driver.back()

    ch.click()
    # time.sleep(3)

data = {"Company Name":list_companies,"Domain Name":domain_name}
df = pd.DataFrame(data)
print(df)
df.to_csv('Companies_and_domains.csv',index=False)
print(f"Total Companies: {len(list_companies)}")
