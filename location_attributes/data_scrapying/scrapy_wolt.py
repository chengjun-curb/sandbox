# -*- coding: utf-8 -*-


import json
import os
import pandas as pd
import random
import requests
import time


WOLT_RESTAURANT_DATA = 'wolt_restaurant_data'
WOLT_MENU_DATA = 'wolt_menu_data'

def get_json(plz, lat, lon):
    time.sleep(random.random()+4)
    url = f'https://restaurant-api.wolt.com/v1/pages/delivery?lat={lat}&lon={lon}'
    r = requests.get(url)
    if r.status_code == 200:
        fname = f"./{WOLT_RESTAURANT_DATA}/{lat}_{lon}.json"
        open(fname , 'wb').write(r.content)
    if r.status_code != 200:
        print(f"Warning, Warning, Warning: {r.status_code}")

if False:
    print('Scrape Wolt restaurant id...')
    df = pd.read_json('../income/plz_sampled_points_10.json', dtype={'plz': str})

    for row in df.itertuples():
        plz = row.plz
        print(f'Wolt: {plz}')
        for p in row.sampled_points[1:3]:
            lat = p[1]
            lon = p[0]
            get_json(plz, lat, lon)
    print('DONE')

if False:
    print('Extract Wolt restaurant id...')
    location_list = []
    directory = f"./{WOLT_RESTAURANT_DATA}"
    for filename in os.listdir(directory):
        if filename.endswith(".json"):
            path = os.path.join(directory, filename)
            j = json.load(open(path))
            try:
                for i in j['sections'][0]['items']:
                    restaurant_id = i['venue']['id']
                    name = i['venue']['name']
                    address = i['venue']['address']
                    city = i['venue']['city']
                    lon = i['venue']['location'][0]
                    lat = i['venue']['location'][1]
                    data = [restaurant_id, name, address, city, lat, lon]
                    location_list.append(data)
            except:
                print("No platform there yet!")

    cols = ['restaurant_id', 'name', 'address', 'city', 'lat', 'lon']
    df = pd.DataFrame(location_list, columns=cols)
    df = df.drop_duplicates()
    print(df.shape)
    df.to_csv('./wolt_restaurant.csv')

if False:
    print('Download Wolt endpoints...')
    df = pd.read_csv('./wolt_restaurant.csv')
    for i in df.restaurant_id:
        print(i)
        url = f'https://restaurant-api.wolt.com/v4/venues/{i}/menu'
        r = requests.get(url)
        fname = f'./{WOLT_MENU_DATA}/{i}.json'
        open(fname , 'wb').write(r.content)
        time.sleep(random.random()+1)
