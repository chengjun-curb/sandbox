# coding: utf-8


import pandas as pd
import os
import requests


df = pd.read_json('../plz_sampled_points_10.json', dtype={'plz': str})


headers = {
    'authority': 'cw-api.takeaway.com',
    'method': 'GET',
    # :path: /api/v28/restaurants?deliveryAreaId=1216953&postalCode=09111&lat=50.835054583230196&lng=12.924312310000792&limit=0
    'scheme': 'https',
    'accept': 'application/json, text/plain, */*',
    'accept-encoding': 'gzip, deflate, br',
    'accept-language': 'en',
    # cw-true-ip: 185.29.113.70
    'origin': 'https://www.lieferando.de',
    'sec-ch-ua': '" Not A;Brand";v="99", "Chromium";v="96", "Google Chrome";v="96"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': "macOS",
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'cross-site',
    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.55 Safari/537.36',
    'x-country-code': 'de',
    'x-instana-l': '1,correlationType=web;correlationId=72350f1490dd31f6',
    'x-instana-s': '72350f1490dd31f6',
    'x-instana-t': '72350f1490dd31f6',
    'x-requested-with': 'XMLHttpRequest',
    'x-session-id': '2e2fcc75-aab2-4545-9fc3-ec5894e0c390',
}

params = dict(
# (
#     ('deliveryAreaId', '1216953'),
#     ('postalCode', '09111'),
#     ('lat', '50.835054583230196'),
#     ('lng', '12.924312310000792'),
#     ('limit', '0')
# )
)

for plz in df.plz.unique():
    params['postalCode'] = plz
    print(params)
    response = requests.get('https://cw-api.takeaway.com/api/v28/restaurants', headers=headers, params=params)
    response.raise_for_status()
    fname = f"./lieferando_restaurant_data/{plz}.json"
    open(fname , 'wb').write(response.content)


slug_list = []


directory = "lieferando_restaurant_data"
for filename in os.listdir(directory):
    if filename.endswith(".json"):
#         print(filename)
        fd = open(f'./{directory}/{filename}')
        file_json = json.load(fd)
        restaurants = file_json["restaurants"]
        for restaurant in restaurants:
            slug = restaurants[restaurant]['primarySlug']
            slug_list.append(slug)

print(len(slug_list))
print(len(set(slug_list)))


headers = {
    'authority': 'cw-api.takeaway.com',
    'method': 'GET',
# 'path': /api/v28/restaurant?slug=blizzeria-chemnitz
'scheme': 'https',
'accept': 'application/json, text/plain, */*',
'accept-encoding': 'gzip, deflate, br',
'accept-language': 'en',
# cw-true-ip: 185.29.113.70
'origin': 'https://www.lieferando.de',
'sec-ch-ua': '" Not A;Brand";v="99", "Chromium";v="96", "Google Chrome";v="96"',
'sec-ch-ua-mobile': '?0',
'sec-ch-ua-platform': "macOS",
'sec-fetch-dest': 'empty',
'sec-fetch-mode': 'cors',
'sec-fetch-site': 'cross-site',
'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.55 Safari/537.36',
'x-country-code': 'de',
# 'x-instana-l': '1,correlationType=web;correlationId=3111baa974118bf8',
# 'x-instana-s': '3111baa974118bf8',
# 'x-instana-t': '3111baa974118bf8',
# 'x-requested-with': 'XMLHttpRequest',
# 'x-session-id': '16ef81aa-9d2f-4ea7-9db7-c676fc6c59fd'
}


params = dict(
#     (
#         ('slug', 'blizzeria-chemnitz'),
#     )
)



for slug in slug_set:
    try:
        params['slug'] = slug
        response = requests.get('https://cw-api.takeaway.com/api/v28/restaurant', headers=headers, params=params)
        response.raise_for_status()
        fname = f"./lieferando_menu_data/{slug}.json"
        open(fname , 'wb').write(response.content)
    except:
        print(f'Skip {slug}')
