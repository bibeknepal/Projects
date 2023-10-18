
from selenium import webdriver
from selenium.webdriver.common.by import By
import time
import pandas as pd
from airflow import DAG
import pendulum
from airflow.operators.python import PythonOperator
import pendulum
from datetime import datetime,timedelta

local_tz = pendulum.timezone('Asia/Kathmandu')


def Collect_data():
    driver = webdriver.Chrome()
    driver.get("https://www.nepalipaisa.com/company/NABIL")
    time.sleep(1)

    price_history_button = driver.find_element(By.XPATH, "//a[@data-tab='price-history' and @href='#c-price']")
    driver.execute_script("arguments[0].scrollIntoView();", price_history_button)
    driver.execute_script("arguments[0].click();", price_history_button)

    data_from = driver.find_element(By.XPATH, "//input[@id = 'txtFromDate']")
    driver.execute_script("arguments[0].scrollIntoView();", data_from)
    time.sleep(1)
    data_from.clear()
    data_from.send_keys('2010-01-01')
    search_button = driver.find_element(By.XPATH, "//button[@id='btnSearch']")
    driver.execute_script("arguments[0].click();", search_button)
    time.sleep(0.1)

    no_of_pages = 306
    final_data = []
    for i in range(no_of_pages):
        next_button = driver.find_element(By.XPATH, "//div[@id = 'divPricePager']//ul//li[contains(@class,'next')]/a")
        table_rows = driver.find_elements(By.XPATH,"//table[@id = 'tblPriceHistory']//tbody//tr")
        for row in table_rows:
            data = {}
            data_ = row.text.split()
            data['S.N.'] = data_[0]
            data['Date'] = data_[1]
            data['Txns'] = data_[2].replace(',','')
            data['High'] = data_[3].replace(',','')
            data['Low'] = data_[4].replace(',','')
            data['Close'] = data_[5].replace(',','')
            data['Volume'] = data_[6].replace(',','')
            data['Turnover'] = data_[7].replace(',','')
            data['Previuos Close']= data_[8].replace(',','')
            data['change in Rs.']= data_[9].replace(',','')
            data['Percent Change']= data_[10].replace(',','')
            final_data.append(data)
        driver.execute_script("arguments[0].click();", next_button)
        time.sleep(0.1)

    df = pd.DataFrame(final_data)
    df.to_csv("Nabil.csv",index=False)
    print(f"Data Collection Successful. Total {len(final_data)} data Collected")

def Clean_data():
    #read and preprocess data
    df = pd.read_csv("Nabil.csv")
    df["Date"]=pd.to_datetime(df["Date"])
    df.index=df["Date"]
    df.drop("Date",axis=1,inplace=True)
    #drop duplicates
    df.drop_duplicates()
    #if there is any null value remove it
    if df.isnull().values.any():
        print("DataFrame contains null values.")
        df.dropna(inplace=True)
        print("Null values removed from the data")
    else:
        print("DataFrame does not contain any null values.")

    #select only relevant data and remove unnecessary column
    features = ['High','Low','Close','Volume','Percent Change']
    cleaned_data = df[features]

    cleaned_data.to_csv("Nabil.csv",index=False)
    print("Data Cleaning Successful.")
    print(cleaned_data.head())
    print(len(cleaned_data))

default_args = {
    'retries':1,
    'retry_delay':timedelta(minutes=2),
    'start_date':local_tz.datetime(2023,10,18)
}

dag = DAG(
    "ETL_DAG",
    default_args=default_args,
    schedule_interval=None,
    catchup=False
)

collect_data = PythonOperator(
    task_id = 'collect_data_from_web',
    python_callable=Collect_data,
    dag = dag
)

clean_data = PythonOperator(
    task_id = 'data_cleaning',
    python_callable=Clean_data,
    dag = dag
)

collect_data>>clean_data


