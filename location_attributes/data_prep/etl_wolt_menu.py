import os
import pandas as pd
import json


menu_list = []
directory = "../data_output/wolt/stores"
for filename in os.listdir(directory):
    if filename.endswith(".json"):
        print(filename)
        restaurant_id, _ = filename.split('.')
        fd = open(f'{directory}/{filename}')
        j = json.load(fd)

        category_dict = {}
        for c in j['categories']:
            category_name = c['name']
            category_id = c['id']
            category_dict[category_id] = category_name

        for i in j['items']:
            menu_id = i['id']
            menu_name = i['name']
            menu_description = i['description']
            menu_base_price = i['baseprice']
            category_id = i['category']
            category_name = category_dict[category_id]
            data = [restaurant_id, menu_id, menu_name, menu_description, menu_base_price, category_id, category_name]
            menu_list.append(data)

cols = ['restaurant_id', 'menu_id', 'menu_name', 'menu_description', 'menu_price', 'category_id', 'category_name']
df = pd.DataFrame(menu_list, columns=cols)
df.to_csv('../data_output/wolt/wolt_menu.csv')
