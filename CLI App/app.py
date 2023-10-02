import typer
import pymongo
from typing import Optional

app = typer.Typer()

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
                print(query)
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

@app.command(help="""Note: Wrap the 'query' argument in single quotations.\n
It should be written in the following format.\n
Example:  '{"rank":{"$gt":1}}'""")

def insert(db:str, coll:str, query:str,insert_many:Optional[bool] = False, url: Optional[str]="mongodb://localhost:27017"):
    try:
        client = pymongo.MongoClient(url)
        database = client[db]
        collection = database[coll]
        if collection is not None:
            print('Database Connected Successfully.')
        try:
            if insert_many:
                result = collection.insert_many(eval(query))
                for doc in result.inserted_ids:
                    print("Data inserted with id:",doc)
            else:
                result = collection.insert_one(eval(query))
                print("Data inserted with id:",result.inserted_id)
        except Exception as e:
            typer.echo('Query Error: '+str(e))
    
    except Exception as e:
        typer.echo("Cannot Connect to Database."+ str(e))



if __name__ == "__main__":
    app()
