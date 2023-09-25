import requests
import random
import pymongo
import typer
from typing import Optional
import re
from bs4 import BeautifulSoup
from urllib.parse import urljoin  # Import urljoin to construct absolute URLs
app = typer.Typer()


def scrape_page(url,save_url = set()):
    if url not in save_url:
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

        movies = soup.find_all('div', class_='lister-item-content')
        for movie in movies:
            data = {}
            data['rank'] = int(movie.find('span', class_='lister-item-index').text.replace('.', ''))
            data['name'] = movie.find('h3', class_='lister-item-header').find('a').text
            data['release_year'] = int(re.findall(r'\d+', movie.find('span', class_='lister-item-year text-muted unbold').text)[0])
            data['duration_in_min'] = int(movie.find('span', class_='runtime').text.replace(' min', ''))
            data['genre'] = movie.find('span', class_='genre').text.strip()
            data['rating'] = movie.find('strong').text
            data['vote_count'] = int((movie.find('p', class_='sort-num_votes-visible').text.split('\n'))[2].replace(',', ''))
            # gross_profit_million_dollar = float(movie.find('p', class_='sort-num_votes-visible').text.split('\n')[4].replace("$", '').replace('M', ''))
            final_data.append(data)
            
        

        next_page = soup.find('a', class_='lister-page-next next-page')
        if next_page:
            next_page_url = urljoin(url, next_page.get('href'))  # Construct absolute URL for the next page
            save_url.add(url)
            final_data.extend(scrape_page(next_page_url))            
        else:
            save_url.add(url)
            print('Done! Finished scraping all pages.')

        return final_data
    else:
        print("Data already Scraped.")


def insert_in_database(host,db,coll,data):
    try:

        client = pymongo.MongoClient(host)
        database = client[db]
        collection = database[coll]
        print('Database Connected')
        try:
            collection.insert_many(data)
            print("Data inserted Successfully")
            print("inserted_data:",data)
        except Exception as e:
            print("Error inserting Data:"+str(e))

    except Exception as e:
        print("Error Connecting to database."+str(e))

@app.command(help = """crawls and inserts data in database\n
             """)
def insert(db:str, coll:str,host: Optional[str]="mongodb://localhost:27017"):
    url= "https://www.imdb.com/search/title/?groups=top_250&sort=boxoffice_gross_us,desc&start=1&ref_=adv_nxt"
    data = scrape_page(url)
    insert_in_database(host,db,coll,data)

@app.command(help="""Note: Wrap the 'query' argument in single quotations.\n
It should be written in the following format.\n
Example:  '{"rank":{"$gt":1}}'""")
def find(db:str, coll:str, query:str,aggregate:Optional[bool]=False, url: Optional[str]="mongodb://localhost:27017"):
    try:
        
        client = pymongo.MongoClient(url)
        database = client[db]
        collection = database[coll]
        print('Database Connected Successfully.')

        if aggregate:
            try:
                result = collection.aggregate(eval(query))
                for doc in result:
                    print(doc)
            except Exception as e:
                print('Error aggregating:'+str(e))
        else:
            try:
                result = collection.find(eval(query))
                for doc in result:
                    print(doc)

            except Exception as e:
                print("Query Error:", str(e))
        
    except Exception as e:
        print("Connection Error:", str(e))



if __name__ == "__main__":
    app()





