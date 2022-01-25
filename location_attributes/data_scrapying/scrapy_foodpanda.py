# -*- coding: utf-8 -*-


from datetime import datetime
import pandas as pd
import pickle
import random
import requests
import time

headers = {
    'authority': 'disco.deliveryhero.io',
    'dps-session-id': 'eyJzZXNzaW9uX2lkIjoiNGUwMGZmNDFmNTBkNDgzMzYxN2I2ZjNkY2JhMWRlMmYiLCJwZXJzZXVzX2lkIjoiMTYzMzQzNjI4Mi44MjAyNDU0MTY2LkJyaHF2bDRnNVciLCJ0aW1lc3RhbXAiOjE2MzcyNzExNDB9',
    'sec-ch-ua-mobile': '?0',
    'x-fp-api-key': 'volo',
    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.69 Safari/537.36',
    'accept': 'application/json, text/plain, */*',
    'x-disco-client-id': 'web',
    'sec-ch-ua-platform': '"macOS"',
    'sec-ch-ua': '"Google Chrome";v="95", "Chromium";v="95", ";Not A Brand";v="99"',
}

params = (
    ('language_id', '3'),
    ('include', 'characteristics'),
    ('dynamic_pricing', '0'),
    ('configuration', 'Original'),
    ('country', 'dl'),
    ('customer_id', ''),
    ('customer_hash', ''),
    ('budgets', ''),
    ('cuisine', ''),
    ('sort', 'rating_desc'),
    ('food_characteristic', ''),
    ('use_free_delivery_label', 'false'),
    ('tag_label_metadata', 'false'),
    ('vertical', 'restaurants'),
    ('customer_type', 'regular'),
)
params = dict(params)


def get_json(plz, lat, lon):
    params['latitude'] = lat
    params['longitude'] = lon
    response = requests.get('https://disco.deliveryhero.io/listing/api/v1/pandora/vendors', headers=headers, params=params)
    response.raise_for_status()
    data = response.json()['data']['items']
    for r in data:
        restaurants.add(r['code'])

if False:
    df = pd.read_json('../income/plz_sampled_points_10.json', dtype={'plz': str})
    restaurants = set()
    for row in df.itertuples():
        plz = row.plz
        print(f'foodpanda: {plz}')
        for p in row.sampled_points:
            lat = p[1]
            lon = p[0]
            get_json(plz, lat, lon)
    print('DONE')
    timestamp = datetime.now().strftime('%Y_%m_%d_%H_%M_%S')
    pickle.dump(restaurants, open(f'./entire_germany_restaurants_{timestamp}.pkl', 'wb'))

if False:
    codes_set = pickle.load(open('entire_germany_restaurants_2021_12_09_11_49_31.pkl', 'rb'))
    for i, code in enumerate(codes_set):
        print(code)
        time.sleep(random.random())
        url = f'https://dl.fd-api.com/api/v5/vendors/{code}?include=menus&language_id=2&dynamic_pricing=0&opening_type=delivery&basket_currency=EUR'
        r = requests.get(url)
        fname = f"./foodpanda_data/{code}.json"
        if r.status_code != 200:
            print(code)
        if r.status_code == 200:
            open(fname , 'wb').write(r.content)

