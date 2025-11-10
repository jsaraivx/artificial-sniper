import requests as req
import pandas as pd
from datetime import datetime
import time


sleep_time = 30
now = datetime.now()
df = pd.DataFrame(pd.read_csv("url_links.csv", header=None, index_col=False))
url_list = df.iloc[1:]

def scrap_data():
    for i, row in url_list.iterrows(): 
        response = req.get(row[0])
        json_data = response.json()
        df = pd.json_normalize(json_data)
        print("====================================")
        print(f"Web scraping from url='{row[0]}.'")
        print(f"Scrapping response: {response.status_code}")
        df.to_json(f"raw_data/{str(datetime.now().strftime("%Y-%m-%d_%H:%M:%S"))}.json")
        print("The data is sucessfully scraped. =D")
        print("====================================")

scrap_data()
# while True:
#     scrap_data()
#     for i in range(sleep_time, 0, -1):
#         print(f"The next scrap will be done on {i} seconds...")
#         time.sleep(1)