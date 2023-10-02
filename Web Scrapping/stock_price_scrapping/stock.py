import requests
import random
from bs4 import BeautifulSoup
from pymongo import MongoClient
import datetime


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
    return final_data

def update_database(data):
    client = MongoClient('mongodb://localhost:27017/')
    db = client['stock_data']
    collection = db['live_market']

    for entry in data:
        existing_entry = collection.find_one({'Symbol':entry['Symbol']})
        if existing_entry:
            collection.update_one({'Symbol':entry['Symbol']},{'$set':entry})
            
        else:
            collection.insert_one(entry)
    
    print("Database updated with new data.")
            


if __name__ == '__main__':
    data = scrape_page()
    update_database(data)
    file = open(r'log.txt','a')
    file.write(f"This Script ran at:  {datetime.datetime.now()}.\n")
