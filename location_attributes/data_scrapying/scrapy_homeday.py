import requests
import random
import time
import pandas as pd

DATA_FOLDER = 'homeday_data'

def get_json(plz, lat, lon):
    url = f'https://preisatlas-api.homeday.de/latest/property_price?latitude={lat}&longitude={lon}&utm_medium=referral&utm_source=www.preisatlas.de&map_layer=standard&marketing_type=sell&property_type=apartment&lat={lat}&lng={lon}'
    r = requests.get(url)
    if r.status_code == 200:
        fname = f'./{DATA_FOLDER}/{plz}_{lat}_{lon}.json'
        open(fname , 'wb').write(r.content)
    else:
        print(f'Warning: {r.status_code} for {lat, lon}')

if False:
    print('Download the Homeday endpoints...')
    df = pd.read_json('./plz_sampled_points_10.json', dtype={'plz': str})
    for row in df.itertuples():
        plz = row.plz
        print(f'homeday: {plz}')
        for p in row.sampled_points[1:3]:
            lat = p[1]
            lon = p[0]
            get_json(plz, lat, lon)
    print('DONE')

