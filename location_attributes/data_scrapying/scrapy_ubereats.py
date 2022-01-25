import numpy as np
import geopandas as gpd

# # Get the store ids
import base64
import json
import requests

ORIGINAL_KEY = 'JTdCJTIyYWRkcmVzcyUyMiUzQSUyMkxpbmRhciVDMyVBNG5nc3YlQzMlQTRnZW4lMjIlMkMlMjJyZWZlcmVuY2UlMjIlM0ElMjJFaVJNYVc1a1lYTERwRzVuYzNiRHBHZGxiaXdnVTNSdlkydG9iMnh0TENCVGQyVmtaVzRpTGlvc0NoUUtFZ2tsVHlMSHRvSmZSaEdiOENiTEJnNGVaeElVQ2hJSm4yai1DRjJkWDBZUnN5RjNXNHltSTRBJTIyJTJDJTIycmVmZXJlbmNlVHlwZSUyMiUzQSUyMmdvb2dsZV9wbGFjZXMlMjIlMkMlMjJsYXRpdHVkZSUyMiUzQTU5LjMzOTM2ODkwNTkzMzYzJTJDJTIybG9uZ2l0dWRlJTIyJTNBMTguMTE5Mjk5NzUyOTgyMjI2JTdE'
ORIGINAL_UEV2_LOC = "%7B%22address%22%3A%7B%22address1%22%3A%22Lindar%C3%A4ngsv%C3%A4gen%22%2C%22address2%22%3A%22Stockholm%22%2C%22aptOrSuite%22%3A%22%22%2C%22eaterFormattedAddress%22%3A%22Lindar%C3%A4ngsv%C3%A4gen%2C%20Stockholm%2C%20Sverige%22%2C%22subtitle%22%3A%22Stockholm%22%2C%22title%22%3A%22Lindar%C3%A4ngsv%C3%A4gen%22%2C%22uuid%22%3A%22%22%7D%2C%22latitude%22%3A59.33936890593363%2C%22longitude%22%3A18.119299752982226%2C%22reference%22%3A%22EiRMaW5kYXLDpG5nc3bDpGdlbiwgU3RvY2tob2xtLCBTd2VkZW4iLiosChQKEgklTyLHtoJfRhGb8CbLBg4eZxIUChIJn2j-CF2dX0YRsyF3W4ymI4A%22%2C%22referenceType%22%3A%22google_places%22%2C%22type%22%3A%22google_places%22%2C%22source%22%3A%22manual_auto_complete%22%2C%22addressComponents%22%3A%7B%22countryCode%22%3A%22SE%22%2C%22firstLevelSubdivisionCode%22%3A%22Stockholms%20l%C3%A4n%22%2C%22city%22%3A%22Stockholm%22%2C%22postalCode%22%3A%22%22%7D%2C%22originType%22%3A%22user_autocomplete%22%7D"


def get_cacheKey(latitude, longitude):
    # decode
    key_decoded = base64.b64decode(ORIGINAL_KEY).decode('utf-8')
    key_dict = requests.utils.unquote(key_decoded)
    key_dict = json.loads(key_dict)
    
    # make the lat, lng change
    key_dict['latitude'] = latitude
    key_dict['longitude'] = longitude
    key_dict = json.dumps(key_dict)
    
    # encode
    key_encoded = requests.utils.quote(key_dict)
    key_encoded = base64.b64encode(str.encode(key_encoded))
    key_encoded = key_encoded.decode('utf8')
    
    cacheKey = key_encoded + "/DELIVERY///0/0//JTVCJTVE////////HOME//"
    return cacheKey


def get_uev2_loc(latitude, longitude):
    # decode
    uev2_decoded = requests.utils.unquote(ORIGINAL_UEV2_LOC)
    uev2_dict = json.loads(uev2_decoded)
    
    # make the lat, lng change
    uev2_dict['latitude'] = latitude
    uev2_dict['longitude'] = longitude
    uev2_dict = json.dumps(uev2_dict)
    
    # encode
    uev2_encoded = requests.utils.quote(uev2_dict)
    return uev2_encoded



# print(get_cacheKey(55.605654915606905, 13.001017509124376))
# print('-'*100)
# print(get_uev2_loc(55.605654915606905, 13.001017509124376))




import requests


def get_store_ids(plz, latitude, longitude):
#     latitude, longitude = 52.51619910453245, 13.401657152467507  
    uev2_loc = get_uev2_loc(latitude, longitude)
    cacheKey = get_cacheKey(latitude, longitude)

    payload = {
        "cacheKey": cacheKey,
               "feedSessionCount":{"announcementCount":0,"announcementLabel":""},
               "userQuery":"","date":"","startTime":0,"endTime":0,"carouselId":"","sortAndFilters":[],"marketingFeedType":"","billboardUuid":"","feedProvider":"","promotionUuid":"","targetingStoreTag":"","venueUuid":"","favorites":"","vertical":"","searchSource":"",
               "getFeedItemType":"DYNAMIC",
               "getFeedItemType":""}
    
    headers = {
    "authority": 'www.ubereats.com',
    'method': 'POST',
    'path': '/api/getFeedV1?localeCode=de',
    'scheme': 'https',
    'accept': '*/*',
    'accept-encoding': 'gzip, deflate, br',
    'accept-language': 'en-US,en;q=0.9',
    'content-length': '819',
    'content-type': 'application/json',
    'cookie': f'uev2.id.xp=b0bfc935-3acc-4187-8d0e-34ed0ab12f2b; dId=de513399-60a3-4447-8518-ef25d0fa43e8; uev2.id.session=74af57a8-d159-462a-8a88-8fdd43bd6013; uev2.ts.session=1642587642391; jwt-session=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpYXQiOjE2NDI1ODc2NDIsImV4cCI6MTY0MjY3NDA0Mn0.Y4OEJbTQrngUMbVD6sz_KK9sycJ7eKeNg2fOc1DyoOg; marketing_vistor_id=7c75422b-b7cd-454e-aee6-2e91f0c2e8c5; uev2.loc={uev2_loc}; uev2.diningMode=DELIVERY',
    'origin': 'https://www.ubereats.com',
    # 'referer': 'https://www.ubereats.com/se/feed?diningMode=DELIVERY&pl=JTdCJTIyYWRkcmVzcyUyMiUzQSUyMkxpbmRhciVDMyVBNG5nc3YlQzMlQTRnZW4lMjIlMkMlMjJyZWZlcmVuY2UlMjIlM0ElMjJFaVJNYVc1a1lYTERwRzVuYzNiRHBHZGxiaXdnVTNSdlkydG9iMnh0TENCVGQyVmtaVzRpTGlvc0NoUUtFZ2tsVHlMSHRvSmZSaEdiOENiTEJnNGVaeElVQ2hJSm4yai1DRjJkWDBZUnN5RjNXNHltSTRBJTIyJTJDJTIycmVmZXJlbmNlVHlwZSUyMiUzQSUyMmdvb2dsZV9wbGFjZXMlMjIlMkMlMjJsYXRpdHVkZSUyMiUzQTU5LjMzOTM2ODkwNTkzMzYzJTJDJTIybG9uZ2l0dWRlJTIyJTNBMTguMTE5Mjk5NzUyOTgyMjI2JTdE',
    'sec-ch-ua': '" Not A;Brand";v="99", "Chromium";v="96", "Google Chrome";v="96"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': "macOS",
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-origin',
    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36',
    'x-csrf-token': 'x',
    'x-uber-xps': '%7B%22eats_web_photo_forward_grid_view%22%3A%7B%22name%22%3A%22enabled%22%7D%2C%22eats_web_quick_add_to_cart%22%3A%7B%22name%22%3A%22enabled%22%7D%2C%22eats_web_coi_checkout_v2%22%3A%7B%22name%22%3A%22treatment%22%7D%2C%22eats_web_storefront_store_info%22%3A%7B%22name%22%3A%22treatment%22%7D%2C%22eats_web_low_courier_supply_ux%22%3A%7B%22name%22%3A%22treatment%22%7D%7D'
    }
    
    
    res = requests.post('https://www.ubereats.com/api/getFeedV1?localeCode=de', 
                      json=payload,
                      headers=headers)
    
    res.raise_for_status()
    fname = f'./output_data/store_ids/store_ids_{plz}_{latitude}_{longitude}.json'
    open(fname , 'wb').write(res.content)
    
    



import pandas as pd

df_plz = pd.read_json('sampled_plz_3stellig.json', dtype={'plz': str})


for i in df_plz.loc[1100:].iterrows():
    plz = i[1][0]
    print(f'Working on {plz} ...')
    points = i[1][1]
    for p in points:
        try:
            latitude = p[1]
            longitude = p[0]
        except:
            print(f'Skip one of {plz} ...')
        get_store_ids(plz, latitude, longitude)


# # Extract the store ids
import os


def json_extract(obj, key):
    """Recursively fetch values from nested JSON."""
    arr = []

    def extract(obj, arr, key):
        """Recursively search for values of key in JSON tree."""
        if isinstance(obj, dict):
            for k, v in obj.items():
                if isinstance(v, (dict, list)):
                    extract(v, arr, key)
                elif k == key:
                    arr.append(v)
        elif isinstance(obj, list):
            for item in obj:
                extract(item, arr, key)
        return arr

    values = extract(obj, arr, key)
    return values


store_ids_list = []
directory = "./output_data/store_ids/"
for filename in os.listdir(directory):
    if filename.endswith(".json"): 
        f = os.path.join(directory, filename)
        fd = open(f, 'rb')
        file_json = json.load(fd)
        store_ids = json_extract(file_json, 'storeUuid')
        store_ids_list += store_ids
        

store_ids_set = set(store_ids_list)
len(store_ids_set)  


# # Get the one store 

import requests

# storeUuid = "03bc4360-9ef1-482c-aacc-da707dbab1ba"

def get_store_json(storeUuid):
    res = requests.post('https://www.ubereats.com/api/getStoreV1?localeCode=de', 
                        json={"storeUuid": storeUuid}, 
                        headers={"content-type": "application/json", "x-csrf-token": "x"})
    res.raise_for_status()
    
    fname = f'./output_data/stores/{storeUuid}.json'
    open(fname , 'wb').write(res.content)
    
    
for i in store_ids_set:
    print(i)
    get_store_json(i)


store_ids_list = []
directory = "./output_data/stores/"
for filename in os.listdir(directory):
    if filename.endswith(".json"): 
        f = os.path.join(directory, filename)
        fd = open(f, 'rb')
        file_json = json.load(fd)
        store_ids = json_extract(file_json, 'storeUuid')
        store_ids_list += store_ids




uber_eats = []

directory = "./output_data/stores/"
for filename in os.listdir(directory):
    if filename.endswith(".json"): 
        f = os.path.join(directory, filename)
        fd = open(f, 'rb')
        file_json = json.load(fd)
        try:
            uuid = json_extract(file_json, 'uuid')[0]
            slug = json_extract(file_json, 'slug')[0]
            postcode = json_extract(file_json, 'postalCode')[0]
            postcode = str(postcode)
            latitude = json_extract(file_json, 'latitude')[0]
            longitude = json_extract(file_json, 'longitude')[0]
            
            data = [uuid, slug, postcode, latitude, longitude]
            uber_eats.append(data)
        except:
            print(filename)




df_uber_eats = pd.DataFrame(uber_eats, columns=['uuid', 'slug', 'zipcode', 'latitude', 'longitude'])

df_uber_eats.to_csv('ubereats_stores.csv', index=False)



# res_dict = json_extract(file_json, 'metaJson')
# menu_dict = json.loads(res_dict[0])

# json_extract(menu_dict, 'hasMenu')

