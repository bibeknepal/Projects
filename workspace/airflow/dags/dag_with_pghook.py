from airflow import DAG 
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
import pendulum
from datetime import datetime,timedelta
import requests
import random
from bs4 import BeautifulSoup
import pandas as pd
import logging,csv

local_tz = pendulum.timezone('Asia/Kathmandu')

defaut_args = {
    "retries":1,
    "retry_delay" : timedelta(minutes=5),
    'start_date': datetime(2023, 10, 12,11,0)
}

dag = DAG("dag_with_postgres_hook",default_args=defaut_args,schedule=None,catchup=False)

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



def create_table():
    hook = PostgresHook(postgres_conn_id='postgres_connection')
    conn = hook.get_conn()
    cursor = conn.cursor()
    
    sql = """CREATE TABLE IF NOT EXISTS public.stock_data (
        symbol character varying,
        LTP real,
        Percentage_Change real,
        High real,
        Low real,
        open real,
        Quantity real
    )"""
    print(f"Executing SQL: {sql}")
    cursor.execute(sql)
    conn.commit()
    logging.info("Table created successfully")


def insert_data():
    hook = PostgresHook(postgres_conn_id='postgres_connection')
    conn = hook.get_conn()
    cursor = conn.cursor()


    with open('scrapped_data.csv', 'r') as f:
        reader = csv.reader(f)
        next(reader)
        for row in reader:
            cursor.execute(
                """insert into stock_data values (%s, %s, %s, %s, %s, %s, %s)""",row
            )
        conn.commit()
        cursor.close()
        logging.info("Inserted Data Successfully.")
        


scrape_data = PythonOperator(
    task_id = 'scrape_data',
    python_callable= scrape_page,
    dag = dag
)


create_table_if_not_present = PythonOperator(
    task_id = 'create_table',
    python_callable= create_table,
    dag = dag,
    provide_context=True,
)
insert_data_in_table = PythonOperator(
     task_id = 'insert_data',
     python_callable= insert_data,
     dag = dag
)

scrape_data >>create_table_if_not_present >> insert_data_in_table
