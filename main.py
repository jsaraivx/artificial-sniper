import requests as req
import pandas as pd
from datetime import datetime
import time


sleep_time = 30
now = datetime.now()

def scrap_data():
    url = "https://sb2frontend-altenar2.biahosted.com/api/widget/GetLiveEvents?culture=pt-BR&timezoneOffset=180&integration=mcgames2&deviceType=1&numFormat=en-GB&countryCode=BR&eventCount=0&sportId=0&champIds=11318"
    response = req.get(url)
    json_data = response.json()
    df = pd.json_normalize(json_data)
    print("====================================")
    print("Web scraping from MCGames.")
    print(f"Scrapping response: {response.status_code}")
    df.to_json(f"raw_data/{str(datetime.now())}.json")
    print("The data is sucessfully scraped. =D")
    print("====================================")


while True:
    scrap_data()
    for i in range(sleep_time, 0, -1):
        print(f"The next scrap will be done on {i} seconds...")
        time.sleep(1)