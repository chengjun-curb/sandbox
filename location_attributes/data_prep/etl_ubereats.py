import os
import json
import pandas as pd
from helper import json_extract

uber_eats = []
directory = "../data_output/ubereats/stores/"
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
df_uber_eats.to_csv('../data_output/ubereats_stores.csv', index=False)


