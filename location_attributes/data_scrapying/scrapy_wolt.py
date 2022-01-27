# -*- coding: utf-8 -*-

import json
import os
import pandas as pd
import random
import requests
import time

def get_store_ids(plz, lat, lon):
    time.sleep(random.random()+4)
    url = f'https://restaurant-api.wolt.com/v1/pages/delivery?lat={lat}&lon={lon}'
    r = requests.get(url)
    if r.status_code == 200:
        fname = f"../data_output/wolt/store_ids/store_ids_{plz}_{lat}_{lon}.json"
        open(fname , 'wb').write(r.content)
    if r.status_code != 200:
        print(f"Warning, Warning, Warning: {r.status_code}")

def get_store_json(store_id):
    url = f'https://restaurant-api.wolt.com/v4/venues/{store_id}/menu'
    r = requests.get(url)
    r.raise_for_status()
    fname = f'../data_output/wolt/stores/{store_id}.json'
    open(fname , 'wb').write(r.content)


if __name__ == '__main__':
    df_plz = pd.read_json('../data_output/sampled_plz_2stellig.json', dtype={'plz': str})

    print('Scrape Wolt restaurant id...')
    df_plz = df_plz.loc[df_plz.plz == '10']
    for i in df_plz.iterrows():
        plz = i[1][0]
        print(f'Working on {plz} ...')
        points = i[1][1]
        for p in points:
            try:
                latitude = p[1]
                longitude = p[0]
                print(f'{latitude} - {longitude}')
                get_store_ids(plz, latitude, longitude)
            except:
                print(f'Skip one point in {plz} ...')
    print('Finish scraping the store ids')

    # TODO: refactor this using extract
    print('Extract Wolt restaurant id...')
    location_list = []
    directory = f"../data_output/wolt/store_ids/"
    for filename in os.listdir(directory):
        if filename.endswith(".json"):
            path = os.path.join(directory, filename)
            j = json.load(open(path))
            try:
                for i in j['sections'][0]['items']:
                    restaurant_id = i['venue']['id']
                    slug = i['venue']['slug']
                    name = i['venue']['name']
                    address = i['venue']['address']
                    city = i['venue']['city']
                    lon = i['venue']['location'][0]
                    lat = i['venue']['location'][1]
                    data = [restaurant_id, slug, name, address, city, lat, lon]
                    location_list.append(data)
            except:
                print("No platform there yet!")

    cols = ['store_id', 'slug', 'name', 'address', 'city', 'lat', 'lon']
    df = pd.DataFrame(location_list, columns=cols)
    df = df.drop_duplicates()
    df.to_csv('../data_output/wolt/wolt_store_ids.csv')

    print('Download Wolt endpoints...')
    df = pd.read_csv('../data_output/wolt/wolt_store_ids.csv')
    for store_id in df['store_id']:
        print(f'scrape :{store_id}')
        get_store_json(store_id)
        time.sleep(random.random()+1)
