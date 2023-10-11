from airflow import DAG
from airflow.operators.python import PythonOperator
import pendulum
from datetime import datetime
import requests
import random
from bs4 import BeautifulSoup
from pymongo import MongoClient
import pandas as pd

local_tz = pendulum.timezone('Asia/Kathmandu')


def scrape_page():
    url = 'https://merolagani.com/LatestMarket.aspx'
    final_data = []
    user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/116.0',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/117.0',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36 Edg/115.0.1901.203'
        ]

    header = {'User-Agent': random.choice(user_agents)}
    source = requests.get(url, headers=header)
    source.raise_for_status()
    soup = BeautifulSoup(source.text, "html.parser")
    table = soup.find('table',class_= 'table table-hover live-trading sortable')
    tbody = table.find('tbody')
    trows = tbody.find_all('tr')
    final_data = []
    for row in trows:
        data ={}
        data['Symbol'] = row.find_all('td')[0].text
        data['LTP'] = float(row.find_all('td')[1].text.replace(',',''))
        data['Percentage_Change'] = float(row.find_all('td')[2].text)
        data['High'] = float(row.find_all('td')[3].text.replace(',',''))
        data['Low'] = float(row.find_all('td')[4].text.replace(',',''))
        data['Open'] = float(row.find_all('td')[5].text.replace(',',''))
        data['Quantity'] = int(row.find_all('td')[6].text.replace(',',''))
        

        final_data.append(data)
    final_data_df = pd.DataFrame(final_data)
    final_data_df.to_csv("scrapped_data.csv",index=False)

        

def update_database():
    df = pd.read_csv("scrapped_data.csv")
    data= df.to_dict(orient='records')

    client = MongoClient('host.docker.internal',27017)
    db = client['stock_data']
    collection = db['live_market']
    try:
        client.server_info()  # This will test the connection
        print("Connected to MongoDB")
    except Exception as e:
        print(f"Connection error: {e}")

    for entry in data:
        existing_entry = collection.find_one({'Symbol':entry['Symbol']})
        if existing_entry:
            collection.update_one({'Symbol':entry['Symbol']},{'$set':entry})
            
        else:
            collection.insert_one(entry)
    
    print("Database updated with new data.")
    


# Define default_args for the DAG
default_args = {
    'start_date': datetime(2023, 10, 10),
    'retries': 1
}

# Create a DAG object
dag = DAG('stock_scraping_dag', default_args=default_args, schedule_interval=None)

# Define tasks
scrape_data = PythonOperator(
    task_id = 'scrape_data',
    python_callable= scrape_page,
    dag = dag
)

update_db = PythonOperator(
    task_id = 'update_db',
    python_callable= update_database,
    dag = dag
)

# Set task dependencies
scrape_data >> update_db
