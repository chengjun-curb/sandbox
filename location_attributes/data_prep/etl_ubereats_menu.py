import os
import json
import pandas as pd

uber_eats = []
directory = "../data_output/ubereats/stores/"
for filename in os.listdir(directory):
    if filename.endswith(".json"):
        print(filename)
        f = os.path.join(directory, filename)
        fd = open(f, 'rb')
        file_json = json.load(fd)

        restaurant_id = file_json['data']['uuid']
        category_dict = file_json['data']['subsectionsMap']

        for i in file_json['data']['sectionEntitiesMap'].items():
            key = i[0]
            data = i[1]
            for item in data.items():
                menu_id = item[0]
                item_data = item[1]
                menu_title = item_data['title']
                menu_price = item_data['price']
                try:
                    menu_description = item_data['description']
                except:
                    menu_description = None

                for c in category_dict:
                    if menu_id in category_dict[c]['itemUuids']:
                        category_name = category_dict[c]['title']
                        category_id = category_dict[c]['uuid']


                row_data = [restaurant_id, menu_id, menu_title, menu_description, menu_price, category_id, category_name]
                uber_eats.append(row_data)


df_ubereats_menu = pd.DataFrame(uber_eats, columns=['restaurant_id', 'menu_id', 'menu_name', 'menu_description', 'menu_price',  'category_id', 'category_name'])
df_ubereats_menu.to_csv('../data_output/ubereats/ubereats_menu.csv')
