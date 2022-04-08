import schedule
import requests
import time

import random


def requestPost():
    # Create new list of pt and the date which coresponds to the index
    new_prices = [random.uniform(0.6, 2) for _ in range(400)]
    new_date = random.randint(5000, 6000)

    # Url where the post must be sent
    url = 'http://127.0.0.1:5000/prices/<name>'
    url = url.replace('<name>',str(new_date))
    myobj = {'prices': new_prices}

    # Send post request
    requests.post(url, json=myobj)

# Request scheduled for every 20 seconds
schedule.every(20).seconds.do(requestPost)

while True:
    schedule.run_pending()
    time.sleep(1)